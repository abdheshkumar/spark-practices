import util.Boot
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.io.Source
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import scala.util.Try
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders
import java.{util => ju}
import java.sql.Timestamp
import _root_.org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType

object InferSchema extends App with Boot {
//DataFrame is a distributed collection of tabular data organized into rows and named columns
  case class Address(street: String, postcode: String)
  case class Emp(name: String, salary: Long, address: Address)

  def time[A](name: String)(body: => A) = {
    val start = System.currentTimeMillis()
    body
    val end = System.currentTimeMillis()
    println(s"$name Took ${end - start} millis")
  }

  // time("inferSchema") {
  //   val employeeDf = spark.read
  //   //.option("inferSchema", "true")
  //     .json("src/main/resources/employees.json")
  //   import spark.implicits._
  //   val r = employeeDf.as[Emp]
  //   r.collect().toList.foreach(println)
  // }
  case class Developer(
      name: String,
      department: String,
      years_of_experience: Int,
      dob: Timestamp
  )

  time("inferSchema = true") {
    val developerDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd")
      .csv("src/main/resources/engineer.csv")
    import spark.implicits._
    val developerDS = developerDF.as[Developer]
    developerDS.collect().toList.foreach(println)
  }

  // Thread.sleep(Int.MaxValue)
  time("inferSchema = false") {
    val schema = StructType(
      List(
        StructField("name", StringType, false),
        StructField("department", StringType, false),
        StructField("years_of_experience", IntegerType, false),
        StructField("dob", TimestampType, false)
      )
    )
    val developerDF = spark.read
      .option("header", "true")
      .schema(schema)
      //.option("timestampFormat", "yyyy-MM-dd")
      .csv("src/main/resources/engineer.csv")
    import spark.implicits._
    val developerDS = developerDF.as[Developer]
    developerDS.collect().toList.foreach(println)
  }

  time("Infer schema from first row") {
    val developerDF1RowSchema = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd")
      .csv("src/main/resources/engineer.csv")
      .head()
      .schema

    val developerDF = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd")
      .schema(developerDF1RowSchema)
      .csv("src/main/resources/engineer.csv")

    import spark.implicits._
    val developerDS = developerDF.as[Developer]
    developerDS.collect().toList.foreach(println)
  }

  // implicit val encoderEmp = Encoders.product[Emp]
  // time("Explicit inferSchema") {
  //   val sc = spark.read
  //     .schema(encoderEmp.schema)
  //     .json("src/main/resources/employees.json")
  //   //import spark.implicits._
  //   val r = sc.as[Emp]
  //   r.collect()
  // }

//   val schemaJson = employeeDf.schema.json
//   println(schemaJson)

//   val file = new File("src/main/resources/employees-schema.json")

//   val bw = new BufferedWriter(new FileWriter(file))
//   bw.write(schemaJson)
//   bw.close()

//   val schemaJsonRead = Source
//     .fromFile("src/main/resources/employees-schema.json")
//     .getLines
//     .mkString

//   val schemaStructType = Try(DataType.fromJson(schemaJsonRead))
//     .getOrElse(LegacyTypeStringParser.parse(schemaJsonRead)) match {
//     case t: StructType => t
//     case _             => throw new RuntimeException(s"Failed parsing JSON Schema")
//   }

//   val empDf = spark.read
//     .schema(schemaStructType)
//     .json("src/main/resources/employees.json")
//   empDf.show()
}

//Have you checked what is the time overhead for interfering the schema automatically vs providing from file
//https://stackoverflow.com/questions/33878370/how-to-select-the-first-row-of-each-group/33878701
//https://stackoverflow.com/questions/32902982/dataframe-dataset-groupby-behaviour-optimization
//https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html
//https://www.youtube.com/watch?v=uxuLRiNoDio
////Using Scala Hack
//import org.apache.spark.sql.catalyst.ScalaReflection
//val scalaSchema = ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]
//Using encoders
//val encoderSchema = Encoders.product[Employee].schema

//https://docs.databricks.com/_static/notebooks/transform-complex-data-types-scala.html
//https://bigdataprogrammers.com/spark-scenario-based-interview-questions/
//http://www.hadoopexam.com/spark/training/docs/Apache_Spark_Interview_Questions_Book.pdf
//https://c2fo.io/c2fo/spark/aws/emr/2016/07/06/apache-spark-config-cheatsheet/
