package bykey

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import util.Boot

case class StudentDetails(studentName: String, subject: String, score: Float)

object CombineByKey extends Boot {

  val students: RDD[(String, StudentDetails)] = spark.sparkContext
    .parallelize(
      List(
        StudentDetails("A", "Math", 68),
        StudentDetails("A", "English", 68),
        StudentDetails("B", "Math", 75),
        StudentDetails("B", "English", 75),
        StudentDetails("C", "Math", 75),
        StudentDetails("C", "English", 56),
        StudentDetails("D", "Math", 89),
        StudentDetails("D", "English", 76),
        StudentDetails("A", "English", 68)
      )
    )
    .map(s => (s.studentName, s))
    //.partitionBy(new HashPartitioner(3)) //It will not trigger merge combiner if keys are in same partion
    .cache()

  println(students.getNumPartitions)

  /*
  students.foreachPartition {
    result => println(result.length)
  }

  students.foreachPartition {
    result =>
      result.foreach(println)
  }
   */

  val createCombiner = (student: StudentDetails) => {
    println(
      s"Combiner:for student ${student.studentName}:::${(student.score, 1)}"
    )
    (student.score, 1)
  }

  val mergeValue = (cm: (Float, Int), student: StudentDetails) => {
    println(s"Merge value${(student.studentName, cm)}")
    (cm._1 + student.score, cm._2 + 1)
  }

  val mergeCombiners = (first: (Float, Int), second: (Float, Int)) => {
    println(s"Merge combiner::")
    (first._1 + second._1, first._2 + second._2)
  }
  val resultOfCombine: RDD[(String, (Float, Int))] =
    students.combineByKey(createCombiner, mergeValue, mergeCombiners)
  resultOfCombine.collectAsMap().map(println)

}
