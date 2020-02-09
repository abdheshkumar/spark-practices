import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CoGroupExample extends App {

  val spark =
    SparkSession.builder().appName("DateTime").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  val rdd1 =
    sc.parallelize(Seq(("key1", 1), ("key2", 2), ("key1", 3), ("key3", 8)))

  val rdd2 = sc.parallelize(Seq(("key1", 5), ("key2", 4)))

  val grouped: RDD[(String, (Iterable[Int], Iterable[Int]))] =
    rdd1.cogroup(rdd2)

  grouped.foreach(println)

  val t: RDD[(String, (List[Int], List[Int]))] = grouped.map { x =>
    val key = x._1
    val value = x._2
    val it1 = value._1.toList
    val it2 = value._2.toList

    (key, (it1, it2))
  }

  t.foreach(println)

  val joined: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

  joined.foreach(println)

}
