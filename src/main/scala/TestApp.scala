import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object TestApp extends App {

  val spark =
    SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()
  val arrayStructureData = Seq(
    Row(Row("James", "", "Smith"), List("Java", "Scala", "C++"), "OH", "M"),
    Row(Row("Anna", "Rose", ""), List("Spark", "Java", "C++"), "NY", "F"),
    Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
    Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
    Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
    Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M")
  )

  val arrayStructureSchema = new StructType()
    .add(
      "name",
      new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType)
    )
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),
    arrayStructureSchema
  )
  val df1 = df.select("name").where(col("state") === "NY")
  println("***********DF1*******")
  df1.explain(true)
  val df2 = df.where(col("state") === "NY").select("name")
  println("***********DF2*******")
  df2.explain(true)

}
