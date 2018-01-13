import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction

object RestaurantApp extends Boot {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  case class RestClass(id: String, name: String,
                       addr: String, city: String,
                       phone: String, `type`: String,
                       `className`: String)

  val rest1DS: Dataset[RestClass] = spark
    .read
    .option("quote", "\"")
    .option("header", true)
    .csv("src/main/resources/restaurant/restaurant.csv")
    .as[RestClass]

  def formatPhoneNo(s: String): String =
    if (s.contains("/"))
      s.replaceAll("/", "-").replaceAll("-	", "-").replaceAll("--", "-")
    else s

  val udfStandardizePhoneNos: UserDefinedFunction = udf[String, String](x => formatPhoneNo(x))

  val rest2DSM1 = rest1DS.withColumn("stdphone", udfStandardizePhoneNos(rest1DS.col("phone")))
  rest1DS.createOrReplaceTempView("rest1Table")
  rest2DSM1.createOrReplaceTempView("rest2Table")
  val countDf = spark.sql("SELECT	count(*)	from	rest1Table,	rest2Table	where	rest1Table.phone	=	rest2Table.stdphone")
  countDf.show()

  val	sqlDF	=	spark.sql(
    "SELECT	a.name,	b.name,	a.phone,	b.stdphone	from	rest1Table	a,	rest2Table	b	where	a.phone	=	b.stdphone")
  sqlDF.show()
}
