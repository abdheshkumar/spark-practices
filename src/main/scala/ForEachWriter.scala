import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.ForeachWriter
import util.Boot
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger

object ForEachWriter extends Boot {

  import spark.implicits._

  val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString

  val upstream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[String]

  /*
  // Read all the csv files written atomically in a directory
val userSchema = new StructType().add("name", "string").add("age", "integer")
val csvDF = spark
  .readStream
  .option("sep", ";")
  .schema(userSchema)      // Specify schema of the csv files
  .csv("/path/to/directory")    // Equivalent to format("csv").load("/path/to/directory")
   */
  /*val downstream = upstream
    .repartition(1)
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", checkpointLocation)
    .format("parquet")
    .option("path", "/tmp/data")
    .start()*/
  val downstream = upstream
    .repartition(1)
    .writeStream
    .foreach(new ForeachWriter[String] {
      override def process(value: String): Unit =
        spark.sparkContext.parallelize(Seq(value))
          .saveAsTextFile("/tmt/data/a.txt")

      override def close(errorOrNull: Throwable): Unit = {}

      override def open(partitionId: Long, version: Long): Boolean = true
    })
    .outputMode(OutputMode.Update())
    .trigger(Trigger.ProcessingTime(25, TimeUnit.SECONDS))
    .start()

  downstream.awaitTermination()


}
