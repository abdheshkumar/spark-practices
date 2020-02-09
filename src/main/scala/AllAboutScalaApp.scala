import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object AllAboutScalaApp extends App {

  //Reading csv file

  val ss = SparkSession
    .builder()
    .appName("AllAboutScalaApp")
    .master("local[*]")
    .getOrCreate()

  //val csvDf: DataFrame = ss.read.csv("employee.csv").toDF("name", "rollno", "address")

  val csvDf: DataFrame = ss.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("employee_header.csv")

  val filterDf: Dataset[Row] = ss.read
    .csv("employee.csv")
    .toDF("name", "rollno", "address")
    .filter(("rollno < 500"))

  import org.apache.spark.sql.functions._

  csvDf.select(min("rollno")).show()

  csvDf.describe().show()

  csvDf.createOrReplaceTempView("employee")

  csvDf.select("*").orderBy("name").show()

  csvDf.filter("rollno in (123,786)").show()

  csvDf.groupBy("rollno").count.filter("count < 3").show()

  csvDf.join(filterDf, "name").show

  csvDf.join(filterDf, Seq("name"), "left_outer").show

  csvDf.filter("rollno < 500").filter("name like 'a%'").distinct().show()

  ss.catalog.listTables().show()

  ss.sql("select * from employee").show

  ss.sql("select * from employee order by name desc").show

}
