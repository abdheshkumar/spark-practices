import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, MapType, StringType, StructType}
import util.Boot
import org.apache.spark.sql.functions._
object MapDataFrame extends Boot with App {
  import spark.implicits._
  //Creating DF with MapType
  val arrayStructureData = Seq(
    Row("James", Map("hair" -> "black", "eye" -> "brown")),
    Row("Michael", Map("hair" -> "gray", "eye" -> "black")),
    Row("Robert", Map("hair" -> "brown"))
  )

  val mapType = DataTypes.createMapType(StringType, StringType)

  val arrayStructureSchema = new StructType()
    .add("name", StringType)
    .add("property", MapType(StringType, StringType))

  val mapTypeDF = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),
    arrayStructureSchema
  )
  mapTypeDF.printSchema()
  mapTypeDF.show(false)
  mapTypeDF.select(col("name"),
    col("property").getItem("hair").as("hair_color"),
    col("property").getItem("eye").as("eye_color"))
    .show(false)
  /*
  Note that if a key is not present on any row, getItem() function returns a null value.
  In case if your keys are dynamic and you wanted to automatically find all keys and convert into columns.
   */
  val keysDF = mapTypeDF.select(explode(map_keys($"property"))).distinct()
  val keys = keysDF.collect().map(f=>f.get(0))
  val keyCols = keys.map(f=> col("property").getItem(f).as(f.toString))
  mapTypeDF.select(col("name") +: keyCols:_*).show(false)
}
