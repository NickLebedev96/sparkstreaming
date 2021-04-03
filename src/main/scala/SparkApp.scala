import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object SparkApp {

  val schemaHotel = new StructType(
    Array[StructField](
      StructField("id", DataTypes.LongType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("country", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("city", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("address", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("longitude", DataTypes.DoubleType, nullable = false, Metadata.empty),
      StructField("latitude", DataTypes.DoubleType, nullable = false, Metadata.empty),
      StructField("weatherdate", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("geohash", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("tmpc", DataTypes.DoubleType, nullable = false, Metadata.empty),
      StructField("tmpf", DataTypes.DoubleType, nullable = false, Metadata.empty),
      StructField("count", DataTypes.LongType, nullable = false, Metadata.empty),
    )
  )

  val schemaExpedia = new Schema.Parser()
    .parse(new File("src/main/resources/expedia.avsc"))


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ExpediaSpark")
      .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")


    val kafkaDf = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.30:6667")
      .option("subscribe", "hotels.result5")
      .option("auto.offset.reset", "latest")
      .option("startingOffsets", """{"hotels.result5":{"0":315000}}""")
      .load()


    val hotelsDataStream = getHotelsData(schemaHotel, kafkaDf)


    val expediaSchema = sparkSession.read
      .format("avro")
      .option("avroSchema", schemaExpedia.toString)
      .load("hdfs://localhost:9000/expedia/*.avro")
      .schema

    val expediaStream = sparkSession.readStream
      .schema(expediaSchema)
      .format("avro")
      .option("avroSchema", schemaExpedia.toString)
      .load("hdfs://localhost:9000/expedia/*.avro")
      .select("*")


    val process = processStreaming(hotelsDataStream, expediaStream)


    val query = process.writeStream
      .outputMode("append")
      .queryName("writing_to_es")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "/tmp/456/")
      .start("expedia/doc")

    val query2 = process.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
    query2.awaitTermination()
    query.stop()
    query2.stop()

    sparkSession.close()
  }

  /**
   * Get hotels data from kafka source
   *
   * @param schemaHotel - schema of hotels data
   * @param kafkaDf     - dataframe source with hotels data
   * @return Dataframe with hotels data
   */
  def getHotelsData(schemaHotel: StructType, kafkaDf: DataFrame) = {
    kafkaDf
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schemaHotel).as("data"))
      .select("data.*")
  }

  def processStreaming(hotelStream: DataFrame, expediaStream: DataFrame): DataFrame = {


    val hotelsWithExpedia = expediaStream.join(hotelStream, hotelStream("id") === expediaStream("hotel_id") && hotelStream("weatherdate") === expediaStream("srch_ci"))

    val hotelsAndExpediaByPlusTmp = hotelsWithExpedia.filter(hotelsWithExpedia("tmpc") > 0)

    val dataWithDuration = calculateDuration(hotelsAndExpediaByPlusTmp)
    val dataWithChildrenAndDuration = withChildren(dataWithDuration)
    val dataForProcessing = aggregateDataForProcessing(dataWithChildrenAndDuration)

    val dataSetWithType = mapStayType(dataForProcessing)
      .withWatermark("eventTime", "1 day")
      .groupBy(window(col("eventTime"), "1 day"), col("with_children"), col("stayType"))
      .count()
      .withColumn("start_date", (to_date(col("window.start"))).cast(StringType))
      .withColumn("end_date", (to_date(col("window.end"))).cast(StringType))
    //
    dataSetWithType
  }

  /**
   * select special column for next processing
   *
   * @param dataWithChildrenAndDuration - dataframe there are 'with_children' column and 'duration' column
   * @return dataframe with selected columns 'duration', 'hotel_id', 'with_children', 'eventTime'
   */
  def aggregateDataForProcessing(dataWithChildrenAndDuration: DataFrame): DataFrame = {
    dataWithChildrenAndDuration.select(
      col("duration"),
      col("hotel_id"),
      col("with_children"),
      to_timestamp(
        col("srch_ci")
      ).as("eventTime")
    )
  }

  /**
   * Define with_children column by condition:
   * If 'srch_children_cnt' column > 0 then with_children will true
   * If 'srch_children_cn' column <= 0 then with_children will false
   *
   * @param data - dataframe with 'srch_children_cnt' column
   * @return dataframe with new column 'with_children'
   */
  def withChildren(data: DataFrame): DataFrame = {
    data.withColumn(
      "with_children",
      when(col("srch_children_cnt") > 0, true)
        .when(col("srch_children_cnt") <= 0, false)
    )
  }

  /**
   * Create new column 'duration'
   * Calculate days between check out and check in
   * 
   * @param hotelsAndExpediaByPlusTmp - dataset which contains columns: srch_co and srch_ci
   * @return dataframe with new column 'duration'
   */
  def calculateDuration(hotelsAndExpediaByPlusTmp: Dataset[Row]): DataFrame = {
    hotelsAndExpediaByPlusTmp.withColumn(
      "duration", datediff(
        hotelsAndExpediaByPlusTmp("srch_co"),
        hotelsAndExpediaByPlusTmp("srch_ci")
      )
    )
  }

  /**
   * Create new column 'stayType' with label of type duration
   *
   * @param sourceDF - dataframe consist column 'duration'
   * @return dataframe with new column 'stayType'
   */
  def mapStayType(sourceDF: DataFrame): DataFrame = {
    val cst = udf((duration: Int) => {
      if (duration >= 14 && duration < 28) {
        "Long stay"
      }
      else if (duration >= 7 && duration < 14) {
        "Standard extended stay"
      }
      else if (duration >= 2 && duration < 7) {
        "Standard  stay"
      }
      else if (duration == 1) {
        "Short Stay"
      }
      else {
        "Erroneous data"
      }
    })
    sourceDF.withColumn("stayType", cst(col("duration")))
  }

}


