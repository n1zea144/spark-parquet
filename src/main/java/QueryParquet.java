import org.apache.spark.sql.*;

public class QueryParquet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Query Parquet").getOrCreate();
        Dataset<Row> df = spark.read().parquet(args[0]);
        df.createOrReplaceTempView("mutations");
        Dataset<Row> result = spark.sql("select distinct Variant_Classification from mutations");
        result.show();
    }
}
