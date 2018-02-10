package streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafkaWordCount extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("test")

  val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

  messages.map(_.value()).foreachRDD { rdd =>
    println("-------------------------" + rdd.collect().toList)
    if (!rdd.isEmpty())
      rdd.coalesce(1).saveAsTextFile("/tmp/data-a/")
  }

  /* messages.foreachRDD { rdd =>
     val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
     rdd.foreachPartition { iter =>
       val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
       println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
     }
   }*/

  // Start the computation
  ssc.start()
  ssc.awaitTermination()
}
