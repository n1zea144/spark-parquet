import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object QueryParquet {
       def main(args: Array[String]) {
           val spark = SparkSession.builder.appName("Query Parquet").getOrCreate()
           val df = spark.read.parquet(args(0))
           //df.printSchema()
           df.createOrReplaceTempView("mutations")
           val result = spark.sql("select distinct Variant_Classification from mutations")
           spark.time(result.show())
       }
}
