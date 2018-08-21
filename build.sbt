import sbt.Keys.version

val sparkV = "2.2.1"

val circeVersion = "0.9.3"

val circe = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

lazy val root = Project("root", file("."))
  .settings(
    name := "spark-practices",
    version := "0.1",
    scalaVersion := "2.11.4",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkV,
      "org.apache.spark" %% "spark-sql" % sparkV,
      "org.apache.spark" %% "spark-streaming" % sparkV,
      "org.apache.spark" %% "spark-mllib" % sparkV,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkV
    ) ++ circe
  )


        