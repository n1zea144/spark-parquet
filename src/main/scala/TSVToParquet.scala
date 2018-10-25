import org.apache.spark.sql.SparkSession

object TSVToParquet {
       def main(args: Array[String]) {
           val spark = SparkSession.builder.appName("TSV to Parquet").getOrCreate()
           val df = spark.read.format("csv").option("delimiter", "\t").option("header", "true").load(args(0))
           df.write.parquet(args(1))
       }
}
