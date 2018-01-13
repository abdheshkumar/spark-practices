import org.apache.spark.sql.SparkSession

trait Boot extends App {
  val spark = SparkSession.builder
    .master("local")
    .appName("app-name")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
}
