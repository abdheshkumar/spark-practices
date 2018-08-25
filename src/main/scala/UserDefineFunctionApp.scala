import util.Boot
import org.apache.spark.sql.functions._

object UserDefineFunctionApp extends Boot {
  // Defined a UDF that returns true or false based on some numeric score.
  val predict = udf((score: Double) => if (score > 0.5) true else false)
val data = spark.sparkContext.parallelize(Seq("test"))
  // Projects a column that adds a prediction column based on the score column.
  //df.select(predict(df("score")))
}
