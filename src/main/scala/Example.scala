import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object Example extends App {


  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val df = spark.read.json("src/main/resources/test.json").toDF("customer", "location", "product")

  //Return DataSet of Age column
  val freq: Dataset[Array[String]] = df.select("customer.age").as[Array[String]] //.select("customer.age >= 10")

  //Collect age that might have nulls
  val result: Array[Array[String]] = freq.rdd.collect()

  //Remove null ages
  val notNullResult: List[String] = result.filterNot(_ == null).flatten.toList

  //Get first and second age
  notNullResult match {
    case Nil => println("Empty")
    case first :: Nil => println(s"First::${first}")
    case first :: second :: _ => println(s"First::${first}, Second:: ${second}")
  }
  //Print all ages
  notNullResult.foreach(println)
}
