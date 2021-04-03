import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SparkTest extends FunSuite with SharedSparkContext with BeforeAndAfterEach{

  var session : SparkSession = _

  override def beforeAll(){
    super.beforeAll()
    session = SparkSession.builder().config(sc.getConf).getOrCreate()
  }

  override def afterAll(){
    super.afterAll()
    session.close()
  }


  test("Hotels") {
    val dataFromMockKafka = session.read
      .option("multiline", "true")
      .json("src/test/resources/hotels.json")

    val hotels = SparkApp.getHotelsData(SparkApp.schemaHotel, dataFromMockKafka)
    assert(hotels.count() > 0)
  }

  test("Duration And StayType") {
    val forDuration = getDataFromJson("src/test/resources/duration.json")

    val duration = SparkApp.calculateDuration(forDuration)
    val data = duration.select(col("duration")).collect()(0)(0)
    assert(data === 4)

    val stayType = SparkApp.mapStayType(duration)
    val typeString = stayType.select(col("stayType")).collect()(0)(0)
    assert(typeString === "Standard  stay")
  }


  test("with_children") {
    val forChildren = getDataFromJson("src/test/resources/with_children.json")
    val withChildren = SparkApp.withChildren(forChildren)

    val data = withChildren.select(col("with_children")).collect()
    assert(data(0)(0) === true)
    assert(data(1)(0) === false)
    assert(data(2)(0) === false)
  }


  def getDataFromJson(path: String): DataFrame = {
    session.read
      .option("multiline", "true")
      .json(path)
  }

}
