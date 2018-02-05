import org.apache.spark.sql.streaming.Trigger

object SparkKafkaAggregation extends Boot {

  import spark.implicits._

  val lines = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()
    /*Using DataFrames, DataSets and/or SQL
    Catalyst figures out how to execute the transformation incrementally
    Internal processing always exactly-once
    */
    .groupBy('value.cast("string") as 'key)
    .agg('count ("*") as 'value)
    //Write into Kafka
    .writeStream
    .format("kafka")
    .option("topic", "output")
    /*
    Trigger – When to output
    Specified as a time, eventually
    supports data size
     No trigger means as fast as possible
    */
    .trigger(Trigger.ProcessingTime("10 seconds"))
    /*
    Complete – Output the whole answer every time
    Update – Output changed rows
    Append – Output new rows only
     */
    .outputMode("update")
    /*
  Tracks the progress of a query in persistent storage
  Can be used to restart the query if there is a failure
  Fault-tolerance with Checkpointing
  Offsets and metadata saved as JSON
  Can resume after changing your
  streaming transformations
  end-to-end exactly-once guarantees
     */
    .option("checkpointLocation", "/tmp/data")

  lines.start()
}
