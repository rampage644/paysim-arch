import org.apache.spark.sql.SparkSession


case class Transaction(
  step: Integer,
  ttype: String,
  amount: Double,
  nameOrig: String,
  oldbalanceOrg: Double,
  newbalanceOrig: Double,
  nameDest: String,
  oldbalanceDest: Double,
  newbalanceDest: Double,
  isFraud: Integer,
  isFlaggedFraud: Integer
)

object TaskN5 {
  def main(args: Array[String]) {
    val logFile = args(0)
    val spark = SparkSession.builder.appName("Task #5").getOrCreate()
    import spark.implicits._
    val logData = spark.read.format("csv").option("header", "true").load(logFile).map(row => Transaction(
      row.getString(0).toInt, // step
      row.getString(1), // ttype
      row.getString(2).toDouble, // amount
      row.getString(3), // name
      row.getString(4).toDouble, // old
      row.getString(5).toDouble, // new
      row.getString(6), // name
      row.getString(7).toDouble, // old
      row.getString(8).toDouble, // new
      row.getString(9).toInt, // fraud
      row.getString(10).toInt// flag fraud
    )).cache()

    val payments = logData.filter(tr => tr.ttype == "PAYMENT").groupBy("nameOrig").sum("amount")
    payments.write.format("csv").option("header", "true").save("payments")

    val topActiveAccounts = logData.groupBy("nameOrig").count().orderBy("count").show(10)
    spark.stop()
  }
}
