import org.apache.spark.sql.*;

public class TSVToParquet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("TSVToParquet").getOrCreate();
        Dataset<Row> df = spark.read().format("csv").option("delimiter", "\t").option("header", "true").load(args[0]);
        df.write().parquet(args[1]);
    }
}
