import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object ChainCustomDFTransformations extends App {

  val spark =
    SparkSession.builder()
      .appName("DataFrame-ComplexSchema")
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._


  def withGreeting(df: DataFrame): DataFrame =
    df.select(concat($"name",lit("greeting")).as("greeting-name"))


  def withFarewell(df: DataFrame): DataFrame = {
    df.withColumn("farewell", lit("goodbye"))
  }


  val df = Seq(
    "shikha",
    "abd"
  ).toDF("name")

  df.transform(withGreeting)
    .transform(withFarewell).show()

}
