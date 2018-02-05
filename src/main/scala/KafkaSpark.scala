import java.util.UUID

object KafkaSpark extends Boot {

  import spark.implicits._

  val lines = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[String]

  val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
  // Generate running word count
  val wordCounts = lines.flatMap(_.split(" ")) //.groupBy("value").count()

  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .outputMode("update")
    .format("console")
    //.option("checkpointLocation", checkpointLocation)
    .start()

  val query1 = wordCounts.writeStream
    .format("parquet")
    .option("path", "/tmp")
    .option("checkpointLocation", checkpointLocation)
    .start()

  val streamdata = spark.read.parquet("/tmp")
  streamdata.foreach(f => println(":::::::::::" + f))
  query1.awaitTermination()
  /*
  df.writeStream
        .format("kafka")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        // We need to reduce blocking time to efficiently test non-existent partition behavior.
        .option("kafka.max.block.ms", "1000")
        .trigger(Trigger.Continuous(1000))
        .queryName("kafkaStream")
   */
  query.awaitTermination()

}
