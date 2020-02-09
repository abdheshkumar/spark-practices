import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object DataFrameApp {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()
    
    val empCsvDF: DataFrame = ss.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv("employee.csv")
      .toDF("name","rollno","address")

    empCsvDF.printSchema()

    empCsvDF.show()

    empCsvDF.select("name","rollno").show()

    import ss.implicits._

    empCsvDF.select($"name".as[String], ($"rollno"+ 1).as("newrollno")).show()

    empCsvDF.filter($"rollno" === 788).show()

    empCsvDF.filter("rollno == 788").show()

    empCsvDF.where($"rollno" === 788).show()

    empCsvDF.filter($"rollno".equalTo(788)).show()

    empCsvDF.groupBy("name").count().show()

    empCsvDF.createOrReplaceTempView("employee")

    ss.sql("Select * from employee").show()

    empCsvDF.createGlobalTempView("employeeG")

    ss.sql("Select * from global_temp.employeeG").show()

    ss.newSession().sql("SELECT * FROM global_temp.employeeG").show()

    ss.catalog.listTables().show()

  }



}
