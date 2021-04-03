name := "sparkstreaming"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-avro" % "2.4.4",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4" ,
  "org.apache.kafka" % "kafka-clients" % "2.7.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.4_0.14.0" % Test,
"org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided",
"org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.12.0"
)

