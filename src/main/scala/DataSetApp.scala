import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import util.Boot

object DataSetApp extends Boot {

  import spark.implicits._

  val rdd: RDD[String] = spark.sparkContext.textFile("src/main/resources/breast-cancer-wisconsin.csv")

  val cancerRdd: RDD[CancerClass] = rdd.map(_.split(","))
    .map(attributes =>
      CancerClass(
        attributes(0).trim.toLong, attributes(1).trim.toInt,
        attributes(2).trim.toInt, attributes(3).trim.toInt,
        attributes(4).trim.toInt, attributes(5).trim.toInt,
        attributes(6).trim.toInt, attributes(7).trim.toInt,
        attributes(8).trim.toInt, attributes(9).trim.toInt,
        attributes(10).trim.toInt))

  val ds: Dataset[CancerClass] = cancerRdd.toDS()

  val cancerDS: Dataset[CancerClass] = ds.as[CancerClass]

  def binarize(s: Int): Int = s match {
    case 2 => 0
    case 4 => 1
  }

  ds.createOrReplaceTempView("cancerTable")

  //Register a function with sql
  spark.udf.register("udfValueToCategory", (arg: Int) => binarize(arg))
  spark.catalog.listTables.show()
  val sqlUDF: DataFrame = spark.sql("SELECT *, udfValueToCategory(clas) from cancertable")
  cancerDS.show()
}

case class CancerClass(sample: Long, cThick: Int,
                       uCSize: Int, uCShape: Int,
                       mAdhes: Int, sECSize: Int,
                       bNuc: Int, bChrom: Int,
                       nNuc: Int, mitosis: Int,
                       clas: Int)