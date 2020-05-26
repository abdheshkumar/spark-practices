import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import util.Boot
import org.apache.spark.sql.functions._
object ExplodeApp extends Boot with App {
  //explode – spark explode array or map column to rows
  import spark.implicits._

  val arrayData = Seq(
    Row(
      "James",
      List("Java", "Scala"),
      Map("hair" -> "black", "eye" -> "brown")
    ),
    Row(
      "Michael",
      List("Spark", "Java", null),
      Map("hair" -> "brown", "eye" -> null)
    ),
    Row("Robert", List("CSharp", ""), Map("hair" -> "red", "eye" -> "")),
    Row("Washington", null, null),
    Row("Jefferson", List(), Map())
  )

  val arraySchema = new StructType()
    .add("name", StringType)
    .add("knownLanguages", ArrayType(StringType))
    .add("properties", MapType(StringType, StringType))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayData),
    arraySchema
  )
  df.printSchema()
  df.show(false)
  println("**explode – array column example***")
  df.select($"name", explode($"knownLanguages"))
    .show(false)
  println("***explode – map column example*****")
  df.select($"name", explode($"properties"))
    .show(false)
}
