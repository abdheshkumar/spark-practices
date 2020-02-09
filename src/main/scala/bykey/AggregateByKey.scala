package bykey

import util.Boot

object AggregateByKey extends Boot {

  val pairs = spark.sparkContext.parallelize(
    List(("aa", 1), ("bb", 2), ("aa", 10), ("bb", 20), ("aa", 100), ("bb", 200))
  )

  /* aggregateByKey takes an initial accumulator (here an empty list),
     a first lambda function to merge a value to an accumulator, and a
     second lambda function to merge two accumulators */
  pairs
    .aggregateByKey(List[Any]())(
      (aggr, value) => aggr ::: (value :: Nil),
      (aggr1, aggr2) => aggr1 ::: aggr2
    )
    .collectAsMap() //Map(aa -> List(1, 10, 100), bb -> List(2, 20, 200))

  pairs
    .combineByKey(
      (value) => List(value), //createCombiner
      (aggr: List[Any], value) => aggr ::: (value :: Nil), //mergeValue
      (aggr1: List[Any], aggr2: List[Any]) => aggr1 ::: aggr2 //mergeCombiners
    )
    .collectAsMap()

  // scala.collection.immutable.Map[String,List[Any]] =
  //                            Map(aa -> List(1, 10, 100), bb -> List(2, 20, 200))

}
