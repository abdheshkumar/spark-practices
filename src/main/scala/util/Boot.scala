package util

import org.apache.spark.sql.SparkSession

trait Boot {
  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("app-name")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
}
