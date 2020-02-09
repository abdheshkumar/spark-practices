import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountApp extends App{

     val ss = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()
     val sc = ss.sparkContext
//
//  //Rdd Operations
//
//  val textFileRdd= sc.textFile("testfile.txt")
//
//  val wordCounts= textFileRdd.flatMap(line => line.split(" ")).map(s=> (s,1)).reduceByKey((a,b) => a+b)
//  wordCounts.foreach(println)
//
//  import ss.implicits._
//
//  val x = (1 to 10).toList
//  val numbersDf = x.toDF
//
//  println(s"Rdd partitions:::: ${numbersDf.rdd.getNumPartitions}")

  val rdd = sc.parallelize(List(12.0,23.6,56.4))

  println(simpleSparkProgram(rdd))

  Thread.sleep(600000)

  def simpleSparkProgram(rdd : RDD[Double]): Long = {
    rdd.filter(_< 1000.0)
      .map(x => (x, x) )
      .groupByKey()
      .map{ case(value, groups) => (groups.sum, value)}
      .count()
  }
}
