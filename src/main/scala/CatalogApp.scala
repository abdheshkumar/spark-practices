import org.apache.spark.sql.SparkSession

object CatalogApp extends App{
  val spark =
    SparkSession.builder()
      .appName("DataFrame-ComplexSchema")
      .master("local[*]")
      .getOrCreate()

  spark.range(10).createOrReplaceTempView("employee")
  spark.range(20).cache()
  spark.range(30).createOrReplaceGlobalTempView("employee1")
  //spark.catalog.cacheTable("employee")

  spark.catalog.listTables().show()


  spark.catalog.currentDatabase




}
