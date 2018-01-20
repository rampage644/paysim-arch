import org.apache.spark.sql.SparkSession


object Ingest {
  def main(args: Array[String]) {
    val input = args(0)
    val spark = SparkSession.builder.appName("Ingest").getOrCreate()
    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input)
    df.write.format("parquet").option("compression", "gzip").save("hdfs://127.0.0.1/transactions")
    spark.stop()
  }
}
