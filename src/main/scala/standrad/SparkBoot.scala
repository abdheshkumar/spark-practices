package standrad

import org.apache.spark.sql.SparkSession
import standrad.storage.{S3Storage, Storage}

trait SparkBoot {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(appName)
      //.enableHiveSupport()
      .getOrCreate()
    parseAndRun(spark, args)
  }

  def parseAndRun(spark: SparkSession, args: Array[String]): Unit = {
    new CommandLineOptionParser().parse(args, UsageConfig()) match {
      case Some(config) => run(spark, config, new S3Storage(spark))
      case None => throw new IllegalArgumentException("arguments provided to job are not valid")
    }
  }


  def run(spark: SparkSession, config: UsageConfig, storage: Storage)

  def appName: String
}
