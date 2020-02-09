package cassandra
import util.Boot

object CassandraApp extends Boot{
  val df_read = spark.read
    .format("org.apache.spark.sql.cassandra")
    .option("spark.cassandra.connection.host","127.0.0.0")
    .option("spark.cassandra.connection.port","9042")
    .option("keyspace","sparkdb")
    .option("table","survey_results")
    .load()
  df_read.show
}
