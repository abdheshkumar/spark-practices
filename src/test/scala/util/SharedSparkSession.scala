package util

import java.io.File
import java.nio.file.{Path, Paths}

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll {
  self: Suite =>

  val spark: SparkSession = createSparkSession

  /**
    * SharedSparkSession is a spark session usable for unit tests that share a single spark object.
    * By using the same object only one spark session is created for all tests.
    */


  private def resourcesPath: Path = Paths.get(new java.io.File(".").getAbsolutePath, "src", "test", "resources")
  private def warehousePath: String = new File(resourcesPath.toFile, "spark-warehouse").getAbsolutePath
  private def checkpointPath: String = new File(resourcesPath.toFile, "checkpoints").getAbsolutePath

  private[this] def createSparkSession: SparkSession = {
    val session = SparkSession
      .builder()
      .appName("spark_test")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", warehousePath)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(checkpointPath)
    session
  }
}