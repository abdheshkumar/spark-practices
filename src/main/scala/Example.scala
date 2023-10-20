import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

object Example extends App {


  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames

  import spark.implicits._


  /* val r = spark
     .sparkContext
     .textFile("src/main/resources/GlobeDataGenerator.example.json")
     .collect().mkString
   println(r)*/

  case class Test(C1: String, C3: String, TurnIdCount: Long, c: Int)

  val metricDf: DataFrame = spark.createDataFrame(
    List(
      Test("TEL_GLB_GENDER", null, 1, 4),
      Test("test something", "TEL_GLB_GENDER", 3, 5)))

  metricDf.show()

  val selectDF = metricDf
    .filter(col("c") === 4)
    .select("C1")
  selectDF.show()
  /*
    val columns = Seq("C1", "C3")
    val result = metricDf
      .select(concat_ws(" ", columns.map(col): _*) as "data", col("TurnIdCount"),col("c"))
      .groupBy("data")
      .agg(sum($"TurnIdCount" + $"c").as("turn_id_count"))
    result.show()
  */

  /*
  +--------------+-------------+
|          data|turn_id_count|
+--------------+-------------+
|TEL_GLB_GENDER|            4|
+--------------+-------------+
   */

  /*val df = spark.read.json("src/main/resources/test.json").toDF("customer", "location", "product")

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
  notNullResult.foreach(println)*/
}
