import java.util.UUID

import org.apache.spark.sql.ForeachWriter
import util.Boot

object KafkaConsole extends Boot {

  import spark.implicits._

  val lines = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "Airports")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[String]

  val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
  // Generate running word count
  val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

  // Start running the query that prints the running counts to the console
  /*val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .option("checkpointLocation", checkpointLocation)
    .start()*/
  val query = lines.writeStream.foreach(new ForeachWriter[String] {
    override def open(partitionId: Long, version: Long): Boolean = {

      true
    }

    override def process(value: String): Unit = {
      println("::::::"+value)
    }

    override def close(errorOrNull: Throwable): Unit = {}
  }).start()

  query.awaitTermination()
}
