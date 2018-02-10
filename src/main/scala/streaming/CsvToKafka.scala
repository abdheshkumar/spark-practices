package streaming

import java.sql.Timestamp

import org.apache.spark.sql.types._
import util.Boot
import org.apache.spark.sql.functions._

object CsvToKafka extends Boot {

  import spark.implicits._

  val mySchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("year", IntegerType),
    StructField("rating", DoubleType),
    StructField("duration", IntegerType)
  ))

  val streamingDataFrame = spark
    .readStream
    .schema(mySchema)
    .csv("src/main/resources/moviedata.csv")

  streamingDataFrame
    .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "moviedata")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "path to your local dir")
    .start()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "moviedata")
    .load()

  val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
    .select(from_json($"value", mySchema).as("data"), $"timestamp")
    .select("data.*", "timestamp")

  df1.writeStream
    .format("console")
    .option("truncate","false")
    .start()
    .awaitTermination()
}
