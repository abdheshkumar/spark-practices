import org.apache.spark.sql.{Column, Row}
import util.Boot
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable
object SQLMapFunctions extends Boot with App {
  import spark.implicits._
  //Spark SQL map functions map(), map_keys(), map_values(), map_contact(), map_from_entries()

  val structureData = Seq(
    Row("36636", "Finance", Row(3000, "USA")),
    Row("40288", "Finance", Row(5000, "IND")),
    Row("42114", "Sales", Row(3900, "USA")),
    Row("39192", "Marketing", Row(2500, "CAN")),
    Row("34534", "Sales", Row(6500, "USA"))
  )

  val structureSchema = new StructType()
    .add("id", StringType)
    .add("dept", StringType)
    .add(
      "properties",
      new StructType()
        .add("salary", IntegerType)
        .add("location", StringType)
    )

  var df = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData),
    structureSchema
  )
  // df.printSchema()
  //df.select(col("id"),explode(col("properties"))).show(false)
//converts all columns from “properties” struct into map key, value pairs “propertiesmap” column
  val index = df.schema.fieldIndex("properties")
  val propSchema = df.schema(index).dataType.asInstanceOf[StructType]
  var columns = mutable.LinkedHashSet[Column]()
  val cols = mutable.LinkedHashSet[Column]()
  propSchema.fields.foreach(field => {
    columns.add(lit(field.name))
    columns.add(col("properties." + field.name))
    cols.add(col("properties." + field.name))
  })

  df.withColumn("myMap", map(cols.toSeq: _*))
    .select(col("id"), explode(col("myMap")))
    .show(false)
  df = df.withColumn("propertiesMap", map(columns.toSeq: _*)).drop("properties")
  //df.printSchema()
  //df.show(false)

  //df.select(col("id"), map_keys(col("propertiesMap"))).show(false)
  //df.select(col("id"), map_values(col("propertiesMap"))).show(false)
  //df.select(col("id"), explode(col("propertiesMap"))).show(false)
}
