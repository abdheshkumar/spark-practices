import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object DataSetExample extends App {

  case class Customer(Age: List[String],
                      frequency: List[String],
                      Income: List[String], ValueSegment: List[String])

  case class Product(brand123: List[String],
                     category: List[String],
                     class1: List[String],
                     styles: List[String],
                     colorfamily: List[String])

  case class Location(tiers: List[String],
                      top10stores: List[String],
                      region: List[String],
                      StoreName: List[String])

  case class Test(customer: Option[Customer],
                  product: Option[Product],
                  location: Option[Location])

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val df = spark.read.json("src/main/resources/test.json").as[Test]
  df.printSchema()
  //df.collect().foreach(println)
}
