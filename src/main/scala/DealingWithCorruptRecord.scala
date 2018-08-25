import util.Boot

object DealingWithCorruptRecord extends Boot {
  //Record invalid record
  spark
    .read
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record") // the default can be configured vis spark.sql.columnNameOfCorruptRecord
    .json("src/main/resources/json/data.json")
    .show()

  //Drop invalid json
  spark
    .read
    .option("mode", "DROPMALFORMED")
    .json("src/main/resources/json/data.json")
    .show()

  //Fail fast
  spark
    .read
    .option("mode", "FAILFAST") //com.fasterxml.jackson.core.JsonParseException
    .json("src/main/resources/json/data.json")
    .show()

}
