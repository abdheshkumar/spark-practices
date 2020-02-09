import org.apache.spark.sql.SparkSession

object WindowAggApp  extends App{

  val spark =
    SparkSession.builder()
      .appName("DataFrame-ComplexSchema")
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

  case class Salary(depName: String, empNo: Long, salary: Long)
  val empsalary = Seq(
    Salary("sales", 1, 5000),
    Salary("personnel", 2, 3900),
    Salary("sales", 3, 4800),
    Salary("sales", 4, 4800),
    Salary("personnel", 5, 3500),
    Salary("develop", 7, 4200),
    Salary("develop", 8, 6000),
    Salary("develop", 9, 4500),
    Salary("develop", 10, 5200),
    Salary("develop", 11, 5200)).toDS

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

  val byDepName = Window.partitionBy("depName")

  empsalary.withColumn("avg", avg("salary") over byDepName).show


  val byHTokens = Window.partitionBy($"depName" startsWith "d")

  val byDepnameSalaryDesc = Window.partitionBy('depname).orderBy('salary desc)

  val rankByDepname = rank().over(byDepnameSalaryDesc)
  empsalary.select('*, rankByDepname as 'rank).show


  val dataset = Seq(
    ("Thin",       "cell phone", 6000),
    ("Normal",     "tablet",     1500),
    ("Mini",       "tablet",     5500),
    ("Ultra thin", "cell phone", 5000),
    ("Very thin",  "cell phone", 6000),
    ("Big",        "tablet",     2500),
    ("Bendable",   "cell phone", 3000),
    ("Foldable",   "cell phone", 3000),
    ("Pro",        "tablet",     4500),
    ("Pro2",       "tablet",     6500))
    .toDF("product", "category", "revenue")


  val overCategory = Window.partitionBy('category).orderBy('revenue.desc)

  val ranked = dataset.withColumn("rank", dense_rank.over(overCategory))




  //Pivot

  val visits = Seq(
    (0, "Warsaw", 2015),
    (1, "Warsaw", 2016),
    (2, "Boston", 2017)
  ).toDF("id", "city", "year")

  val x = visits
    .groupBy("city")  // <-- rows in pivot table
    .pivot("year")    // <-- columns (unique values queried)
    .count()


  //rollup

  val sales = Seq(
    ("Warsaw", 2016, 100),
    ("Warsaw", 2017, 200),
    ("Boston", 2015, 50),
    ("Boston", 2016, 150),
    ("Toronto", 2017, 50)
  ).toDF("city", "year", "amount")


  val withRollup = sales
    .rollup("city", "year")
    .agg(sum("amount") as "amount", grouping_id() as "gid")
    .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
    .filter(grouping_id() =!= 3)
    .select("city", "year", "amount")

  val q = sales
    .rollup("city", "year")
    .agg(sum("amount") as "amount")
    .sort($"city".desc_nulls_last, $"year".asc_nulls_last)

}
