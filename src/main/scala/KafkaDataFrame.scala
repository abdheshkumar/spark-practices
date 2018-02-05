import java.util.UUID

import org.apache.spark.sql.ForeachWriter

object KafkaDataFrame extends Boot {

  import spark.implicits._

  val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString

  case class Data(key: Array[Byte],
                  value: Array[Byte],
                  topic: String,
                  partition: Int,
                  offset: Long,
                  timestamp: Long)

  val upstream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "Airports")
    .option("startingOffsets", "earliest")
    .load()
    .as[Data]

  val writer = new ForeachWriter[Data] {
    override def process(value: Data): Unit = println(new String(value.value))

    override def close(errorOrNull: Throwable): Unit = {}

    override def open(partitionId: Long, version: Long): Boolean = true
  }

  val downstream = upstream
    .writeStream
    .foreach(writer)
    .start()
  downstream.awaitTermination()


}

