import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._



object DataFrameWithSchema extends Boot {
  val recordSchema = new StructType()
    .add("sample", "long")
    .add("cThick", "integer")
    .add("uCSize", "integer")
    .add("uCShape", "integer")
    .add("mAdhes", "integer")
    .add("sECSize", "integer")
    .add("bNuc", "integer")
    .add("bChrom", "integer")
    .add("nNuc", "integer")
    .add("mitosis", "integer")
    .add("clas", "integer")

  val df: DataFrame = spark
    .read
    .format("csv")
    .option("header", false)
    .schema(recordSchema)
    .load("src/main/resources/breast-cancer-wisconsin.csv")

  df.createOrReplaceTempView("cancerTable")
  val sqlDF = spark.sql("SELECT sample, bNuc from cancerTable")
  sqlDF.show()
}
