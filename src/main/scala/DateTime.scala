import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object DateTime extends App {

  val spark = SparkSession.builder().appName("DateTime").master("local[*]").getOrCreate()

  val schema = StructType(
    Seq(
      StructField("id", IntegerType, true),
      StructField("dt", DateType, true),
      StructField("ts", TimestampType, true)
    )
  )

  val dateRdd = spark.sparkContext.parallelize(
    Seq(
      Row(
        1,
        Date.valueOf("1999-01-11"),
        Timestamp.valueOf("2011-10-02 09:48:05.123456")
      ),
      Row(
        1,
        Date.valueOf("2004-04-14"),
        Timestamp.valueOf("2011-10-02 12:30:00.123456")
      ),
      Row(
        1,
        Date.valueOf("2008-12-31"),
        Timestamp.valueOf("2011-10-02 15:00:00.123456")
      )
    ), 4)

  val dateDf = spark.createDataFrame(dateRdd, schema)

  import org.apache.spark.sql.functions._
  import spark.implicits._

  println("DataFrame with both DateType and TimestampType")
  dateDf.show()

  println("Pull a DateType apart when querying")
  dateDf.select($"id",
    month($"dt") as "date_month",
    month($"ts" as "timestamp_month"),
    dayofweek($"dt") as "day_of_week",
    date_add($"dt",10) as "date_with_10days",
    datediff(current_date(),$"dt") as "date_diff"
  ).show()


  println("Date truncation")
  dateDf.select($"dt", trunc($"dt", "YYYY") as "yyyy_dt", trunc($"dt", "YY") as "yy_dt", trunc($"dt", "MM") as "mm_dt").show()

  println("Date formatting")
  dateDf.select($"dt", date_format($"dt", "MMM dd, YYYY")).show()

  println("Pull a Timestamp type apart when querying")


}
