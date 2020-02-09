import sbt.Keys.version

val sparkV = "2.4.5"
val scalaTestV = "3.1.0"
val scalacheckV = "1.14.3"

lazy val root = Project("root", file("."))
  .settings(
    name := "spark-practices",
    version := "0.1",
    scalaVersion := "2.12.10",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkV,
      "org.apache.spark" %% "spark-sql" % sparkV,
      "org.apache.spark" %% "spark-streaming" % sparkV,
      "org.apache.spark" %% "spark-mllib" % sparkV,
      //"com.datastax.spark" %% "spark-cassandra-connector" % sparkV,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV, //Structured Streaming + Kafka Integration Guide
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkV, //spark-streaming-kafka
      "com.github.scopt" %% "scopt" % "3.7.1",
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "org.scalacheck" %% "scalacheck" % scalacheckV % Test
    )
  )
