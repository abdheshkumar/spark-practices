import sbt.Keys.version

val sparkV = "3.5.2"
val scalaTestV = "3.2.19"
val scalacheckV = "1.18.1"

lazy val root = project.in(file("."))
  .settings(
    name := "spark-practices",
    version := "0.1",
    scalaVersion := "2.13.14",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkV,
      "org.apache.spark" %% "spark-sql" % sparkV,
      "org.apache.spark" %% "spark-streaming" % sparkV,
      "org.apache.spark" %% "spark-mllib" % sparkV,
      //"com.datastax.spark" %% "spark-cassandra-connector" % sparkV,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV, //Structured Streaming + Kafka Integration Guide
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkV, //spark-streaming-kafka
      "com.github.scopt" %% "scopt" % "4.1.0",
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "org.scalacheck" %% "scalacheck" % scalacheckV % Test
    )
  )
