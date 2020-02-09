import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lower, regexp_replace, udf}

import scala.util.{Failure, Success, Try}

object UDFApp extends App{

  val ss = SparkSession.builder().master("local[*]").appName("UDFApp").getOrCreate()

  val empDF: DataFrame = ss.read
    .option("multiline", true)
    .option("mode", "PERMISSIVE")
    .json("employee.json")

  def bestLowerRemoveAllWhitespace1(col: Column): Column =
    lower(regexp_replace(col, "\\s+", ""))


  def lowerRemoveAllWhitespace(s: String): String = {
    println("calling udf")
    Try(s.replaceAll("\\s+", "")) match {
      case Success(s) => s
      case Failure(ex) => "not-present"
    }
  }

  val lowerRemoveAllWhitespaceUDF: UserDefinedFunction = udf[String, String](lowerRemoveAllWhitespace)

  import ss.implicits._

  empDF.select(
    $"name",
    $"rollno",
    lowerRemoveAllWhitespaceUDF($"address.street_no").as("street_no"),
    lowerRemoveAllWhitespaceUDF($"address.house_no").as("house_no")).show()

}
