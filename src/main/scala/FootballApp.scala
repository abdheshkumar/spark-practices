import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FootballApp extends App {

  val ss = SparkSession.builder().appName("FootballApp").master("local[*]").getOrCreate()
  import ss.implicits._

  def footballDf: SparkSession => DataFrame = (ss: SparkSession)=> ss.read
    .option("header", "true")
    .option("dateFormat", "dd/mm/yy")
    .option("inferSchema", "true")
    .csv("football-league-2017.csv")
    .toDF("div", "date", "hometeam", "awayteam", "fthg", "ftag", "ftr", "HTHG", "HTAG", "HTR", "Referee", "HS", "AS", "HST", "AST", "HF", "AF", "HC", "AC", "HY", "AY", "HR", "AR", "B365H", "B365D", "B365A", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "LBH", "LBD", "LBA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA", "VCH", "VCD", "VCA", "Bb1X2", "BbMxH", "BbAvH", "BbMxD", "BbAvD", "BbMxA", "BbAvA", "BbOU", "BbMx>2.5", "BbAv>2.5", "BbMx<2.5", "BbAv<2.5", "BbAH", "BbAHh", "BbMxAHH", "BbAvAHH", "BbMxAHA", "BbAvAHA", "PSCH", "PSCD", "PSCA")

  def datadf: DataFrame => Dataset[FootballData] = (footballDf: DataFrame)=>  footballDf.select("date", "hometeam", "awayteam", "fthg", "ftag", "ftr").as[FootballData].cache()

  def pointsToTeamDf: Dataset[FootballData] => DataFrame = (datadf: Dataset[FootballData]) => {
    datadf.flatMap { rec =>
      rec.ftr match {
        case "A" => List(rec.awayTeam -> 3)
        case "H" => List(rec.homeTeam -> 3)
        case "D" => List(rec.homeTeam -> 1, rec.awayTeam -> 1)
      }
    }.toDF("teamname", "point")
  }

  import org.apache.spark.sql.functions._

  def aggPointsDataForTeamDf: DataFrame => DataFrame = (pointsToTeamDf: DataFrame) =>
     pointsToTeamDf
    .groupBy("teamname")
    .agg(sum("point") as "total_points").cache()

  def maxPointsDataForTeamDf: DataFrame => DataFrame = (pointsData: DataFrame) =>
    pointsData.agg(max("total_points") as "total_points")

  def winnerTeamDf: (DataFrame,DataFrame) => DataFrame = (pointsData: DataFrame, maxpoints: DataFrame) =>
     pointsData
    .join(maxpoints, "total_points")
    .select("teamname")

  //Running the pipeline
  import Pimpers._

  val start = System.currentTimeMillis()
  val df1 =footballDf ~ (datadf) ~ pointsToTeamDf ~ aggPointsDataForTeamDf
  val df2: DataFrame => DataFrame = maxPointsDataForTeamDf
  val df3 = winnerTeamDf

  val pointsByTeamInLeague = df1(ss)
  val maxPointsInLeague = df2(pointsByTeamInLeague)
  val result = df3(pointsByTeamInLeague,maxPointsInLeague)

  result.collect.foreach(println(_))

  val end = System.currentTimeMillis()
  println(end-start)

//  //Find Golf Difference if points match
//
//  val winner: String = if (teamsWonByPoints.length >= 2) {
//    val totalHomeTeamGolfs = datadf
//      .join(winnerBasedUponPoints, datadf("hometeam") === winnerBasedUponPoints("teamname"))
//      .groupBy(datadf.col("hometeam"))
//      .agg(sum("fthg") as "sum_fthg")
//
//    val totalAwayTeamGolfs = datadf
//      .join(winnerBasedUponPoints, datadf("awayteam") === winnerBasedUponPoints("teamname"))
//      .groupBy(datadf.col("awayteam"))
//      .agg(sum("ftag") as "sum_ftag")
//
//    import ss.implicits._
//
//    val result = totalHomeTeamGolfs
//      .join(totalAwayTeamGolfs, totalHomeTeamGolfs("hometeam") === totalAwayTeamGolfs("awayteam"))
//      .select($"hometeam" as "team", $"sum_fthg" + $"sum_ftag".as[Int]).as[(String, Int)]
//
//    val finalresult: Array[(String, Int)] = result.collect()
//
//    finalresult.foreach(println)
//
//    val footballResult: (String, Long) = finalresult.toList.foldLeft(("", 0L)) { case (a, b) =>
//      if (b._2 > a._2) (b._1, b._2) else (a._1, a._2)
//    }
//
//    footballResult._1
//
//  }
//  else teamsWonByPoints.head
//
//  println(s"Winner::::$winner")
}

case class FootballData(date: Timestamp, homeTeam: String, awayTeam: String, fthg: Int, ftag: Int, ftr: String)


object Pimpers {
  implicit class SparkSessionPimpers[A](fa: SparkSession=>Dataset[A]){
    def ~[B](f: Dataset[A]=>Dataset[B]): SparkSession=>Dataset[B] = fa andThen(f)
  }
  //  implicit class DataFramePimpers[A,B](fa: Dataset[A]=>Dataset[B]){
  //    def ~>[C](f: Dataset[B]=>Dataset[C]): Dataset[A] => Dataset[C] = fa andThen(f)
  //  }
}
