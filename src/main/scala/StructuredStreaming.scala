import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import util.Boot

object StructuredStreaming extends Boot {

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  /*
   Streaming ETL. Extract, Transform, and Load (ETL)
   Streaming ETL w/ Structured Streaming
   Example
  Json data being received in Kafka Parse nested json and flatten it Store in structured Parquet table  Get end-to-end failure guarantees
   */
  val rawData = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "topic")
    .load()

  val schema = StructType(Seq(
    StructField("schema", StringType, nullable = true),
    StructField("payload", StringType, nullable = true)
  ))

  val parsedData = rawData
    .selectExpr("cast (value as string) as json")
    .select(from_json($"json", schema = schema).as("data"))
    .select("data.payload")

  val query = parsedData.writeStream
    .option("checkpointLocation", "/checkpoint")
    .partitionBy("date")
    .format("parquet")
    /*
    start actually starts a continuous running StreamingQuery in the Spark cluster
     */
    .start("/parquetTable")
  /*
    rawData dataframe has the following columns
    key, value, topic, partition, offset, timestamp
    */
  query.awaitTermination()
}
