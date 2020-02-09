

import org.apache.spark.sql._


object UdfUtis {

  val spark = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()

  def lowerRemoveAllWhitespace(s: String): String = {
    s.toLowerCase().replaceAll("\\s", "")
  }

  spark.udf.register("lowerRemoveAllWhitespaceUDF", lowerRemoveAllWhitespace _)


}
