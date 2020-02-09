import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TestApp extends App {

  val ss = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()
  val sc = ss.sparkContext

  println("hiiiiiiiiiii")

}
