import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}
import util.SharedSparkSession

class CamelCaseParsing extends FlatSpec with SharedSparkSession with Matchers {

  it should "convert into case class using snake case" in {
    import spark.implicits._
    val data: Dataset[User] = spark
      .sparkContext.parallelize(List("name"))
      .toDF("userName")
      .as[User].as
    data.collect().toList shouldBe List(User("name"))
  }


}

case class User(userName: String)
