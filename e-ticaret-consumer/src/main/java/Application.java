import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType()
                .add("search", DataTypes.StringType)
                .add("region", DataTypes.StringType)
                .add("current_ts", DataTypes.StringType)
                .add("userid", DataTypes.IntegerType);

        SparkSession sparkSession = SparkSession.builder().master("local")
                .config("spark.mongodb.output.uri","mongodb://165.232.125.23/eticaret.populerurunler")
                .appName("Spark Search Analysis").getOrCreate();

        Dataset<Row> loadDS = sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "165.232.125.23:9092")
                .option("subscribe", "search-analysis-userid").load();

        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");

        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema)
                .as("jsontostructs")).select("jsontostructs.*");

        //en çok arama yapılan 10 ürün
//        Dataset<Row> searchGroup = valueDS.groupBy("search").count();
//        Dataset<Row> searchResult = searchGroup.sort(functions.desc("count")).limit(10);
//
//        searchResult.show();
//
//        MongoSpark.write(searchResult).mode("overwrite").save();

        Dataset<Row> count = valueDS.groupBy("userid", "search").count();

        Dataset<Row> filter = count.filter("count > 5");

        Dataset<Row> pivot = filter.groupBy("userid").pivot("search").sum("count").na().fill(0);

        pivot.show();

        MongoSpark.write(pivot).option("collection","searchByUserid").mode("overwrite").save();
    }
}
