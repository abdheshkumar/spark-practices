import java.util.UUID

import org.apache.spark.sql.streaming.ProcessingTime

object KafkaToHdfsUsingSpark extends Boot {

  val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString

  val upstream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test,Airport,Airports,Carriers,Planedata")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("topic", "CAST(value AS STRING)")

  val downstream = upstream
    .coalesce(1)
    .writeStream
    .partitionBy("topic") // Partition by topic. it will create directory by topic name topic=Airport,topic=Carriers,topic=Planedata etc
    .format("csv")
    .option("path", "/tmp/data")
    .outputMode("append")
    .trigger(ProcessingTime(3000))
    .option("checkpointLocation", checkpointLocation)
    .start()

  downstream.awaitTermination()


}

