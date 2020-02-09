import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object ComplexSchema extends App {

  val spark =
    SparkSession.builder()
      .appName("DataFrame-ComplexSchema")
      .master("local[4]")
      .getOrCreate()

  val rows1: Seq[Row] = Seq(
    Row(1, Row("a", "b"), 8.00, Row(1, 2)),
    Row(2, Row("c", "d"), 9.00, Row(3, 4))
  )

  val rowsRdd: RDD[Row] = spark.sparkContext.parallelize(rows1)

  val schema: StructType = StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("s1", StructType(
        Seq(
          StructField("x", StringType, true),
          StructField("y", StringType, true)
        )
      ), true),
      StructField("total", DoubleType, true),
      StructField("s2", StructType(
        Seq(
          StructField("x1", IntegerType, true),
          StructField("y1", IntegerType, true)
        )
      ), true)
    )
  )

  val df = spark.createDataFrame(rowsRdd,schema)

  //println(df.printSchema())
  //ArrayType

  val rows11: Seq[Row] = Seq(
    Row(1, Row("a", "b"), 8.00, Array(1, 2)),
    Row(2, Row("c", "d"), 9.00, Array(3, 4))
  )

  val rowsRdd11 = spark.sparkContext.parallelize(rows11)

  val schema11 = StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("s1", StructType(
        Seq(
          StructField("x", StringType, true),
          StructField("y", StringType, true)
        )
      ), true),
      StructField("total", DoubleType, true),
      StructField("s2", ArrayType(IntegerType), true)
    )
  )

  val df1 = spark.createDataFrame(rowsRdd11,schema11)

  import spark.implicits._
  import org.apache.spark.sql.functions._

  df1.select($"id",$"s1".getField("x"),$"s1".getField("y"),$"total",$"s2".getItem(0),$"s2".getItem(1)).show()
  df1.select($"id",size($"s2"),explode($"s2")).show()
}
