import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {

  val spark = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()

  //Text file
  val employeeDf = spark
    .read
    .textFile("employee.txt")

  //Json
  val employeeDf1 = spark
    .read
    .format("json")
    .load("employee.json")

  //parquet
  employeeDf.write
    .mode(SaveMode.Ignore)
    .save("employee.parquet")

  val usersDF = spark.read.load("employee.parquet")

  //Csv
  val employeeDFCsv = spark.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "false")
    .load("employee.csv")

  //Saving to Peristent tables

  employeeDFCsv.write
    .saveAsTable("PermanentEmployee")


  spark.sql("SELECT * FROM PermanentEmployee").show()


  //Schema Merging

  import spark.implicits._

  val squaresDF = (1 to 5).toDS.map(i => (i, i * i)).toDF("value", "square")
  squaresDF.write
      .mode(SaveMode.Ignore)
    .parquet("data/test_table/key=1")


  val cubesDF = (6 to 10).toDS.map(i => (i, i * i * i)).toDF("value", "cube")
  cubesDF.write
    .mode(SaveMode.Ignore)
    .parquet("data/test_table/key=2")

  val parquetFileDF = spark.read
      .option("mergeSchema", "true")
      .parquet("data/test_table")

  parquetFileDF.show()



}
