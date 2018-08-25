package streaming
import util.Boot
import org.apache.spark._
import org.apache.spark.streaming._

object FileStreamingApp extends Boot {

  val data = spark.readStream
    .format("text")
    .load("src/main/resources/text")

  //data.foreach(f => println(f))
  data.writeStream
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination()
}
