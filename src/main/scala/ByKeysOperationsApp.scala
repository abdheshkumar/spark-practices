
import org.apache.spark.sql.SparkSession

object ByKeysOperationsApp extends App {

  val ss = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()
  val sc = ss.sparkContext

  val rdd1 = sc.parallelize(Seq(
    ("key1", 1),
    ("key2", 2),
    ("key1", 3),
    ("key3", 8)))

  /** ReduceByKey
    * Data is combined at each partition , only one output for one key at each partition to send over network. reduceByKey required combining all your values
    * into another value with the exact same type.
    */

  val reduceResult: Array[(String, Int)] = rdd1.reduceByKey((a, b) => a + b).collect()

  reduceResult.foreach(println)

  /**
    * GroupByKey can cause out of disk problems as data is sent over the network and collected combiner Result on the reduce workers.
    */
  val groupResult = rdd1.groupByKey().map { case (x, y) => (x, y.sum) }.collect()


  groupResult.foreach(println)


  /** AggregateByKey same as reduceByKey, which takes an initial value.
    *
    * Inside aggregateByKey , the first lambda function has two parameters. One is the accumulator and the second one is the value of the key on which the aggregateByKey is applied.
    * The accumulator is of type (0,0) , the one we mentioned in aggregateByKey to initialize the accumulator. In the second lambda function it has two parameter
    * which are the outputs of the first lambda function.
    * For the above data (“David”, 6), (“Abby”, 4), (“David”, 5), (“Abby”, 5) , let see the sequence of execution
    * (“David”, 6) --> first lambda function will get the data ((0,0),6)-- here acc=(0,0) is the value of accumulator and value =6 . So the output of the first function is (6,1)
    * (“Abby”, 4)–> first lambda function will get the data ((0,0),4)-- here acc=(0,0) is the value of accumulator and value =4.
    * So the output is (4,1)
    * (“David”, 5) --first lambda function will get the data ((0,0),6)-- here acc=(0,0) is the value of accumulator and value =6 . So the output of the first function is (5,1)
    * (“Abby”, 5)–> first lambda function will get the data ((0,0),4)-- here acc=(0,0) is the value of accumulator and value =4.
    * So the output is (5,1)
    * During reduce phase second lambda function is applied and it will have input like (6,1) and (5,1) and the output will be (David,(11,2))
    */

  //inputdata == Seq((“David”, 6), (“Abby”, 4), (“David”, 5), (“Abby”, 5))   // Array(“David”, (11,2))

  val aggResult: Array[(String, (Int, Int))] = rdd1.aggregateByKey((0, 0))(
    (acc, value) => (acc._1 + value, acc._2 + 1),
    (value1, value2) => (value1._1 + value2._1, value1._2 + value2._2))
    .collect()

  aggResult.foreach(println)


  /**
    * In order to aggregate an RDD’s elements in parallel, Spark’s combineByKey method requires three functions:
    * It’s worth noting that the type of the combined value does not have to match the type of the original value and often times it won’t be
    *
    * createCombiner
    * mergeValue
    * mergeCombiner
    * Create a Combiner
    */


  val combinerResult: Array[(String, (Int, Int))] = rdd1.combineByKey(
    (v) => (v, 1),
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  ).collect()

  combinerResult.foreach(println)


  //result.collectAsMap().map(println(_))


  //rdd1.collectAsMap().foreach(println)

  //rdd1.countByKey().foreach(println)


}
