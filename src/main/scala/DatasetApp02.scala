import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DatasetApp02 extends App {

  val ss = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()
  import ss.implicits._

//////////////// Dataset Operations

  val textFileData: Dataset[String] = ss.read.textFile("testfile.txt")
  textFileData.filter(s => s.contains("spark")).foreach(s => println(s))

  val primitiveDS: Dataset[Int] =  (Seq(1, 2, 3)).toDS()
  primitiveDS.map(_ + 1).foreach(s => println(s))

  val empDF: Dataset[Employee] = ss.read
    .option("multiline", true)
    .option("mode", "PERMISSIVE")
    .json("employee.json")
    .as[Employee]

  empDF.show()

  //Inferring the Schema Using Reflection

  val peopleDF: Dataset[Employee] = ss.read
    .textFile("employee.txt")
    .map(_.split(","))
    .map(attributes => Employee(attributes(0),attributes(1).trim.toInt,attributes(2)))

  peopleDF.createOrReplaceTempView("employee")

  val teenagersDF: DataFrame = ss.sql("SELECT name, rollno FROM employee WHERE rollno BETWEEN 123 AND 786")

  // The columns of a row in the result can be accessed by field index
  teenagersDF.map(teenager => "Name: " + teenager(0)).show()

  // or by field name
  teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()


  // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
//  implicit def mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//  teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "rollno"))).foreach(s => println(s))

}


case class Employee(name: String, rollno: Double, address: String)
