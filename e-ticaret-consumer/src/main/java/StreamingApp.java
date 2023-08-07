import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StreamingApp {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType()
                .add("search", DataTypes.StringType)
                .add("region", DataTypes.StringType)
                .add("current_ts", DataTypes.StringType)
                .add("userid", DataTypes.IntegerType);

        SparkSession sparkSession = SparkSession.builder().master("local")
                .config("spark.mongodb.output.uri","mongodb://165.232.125.23/eticaret.populerurunler")
                .appName("Spark Search Analysis").getOrCreate();

        Dataset<Row> loadDS = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "165.232.125.23:9092")
                .option("subscribe", "search-analysis-stream").load();

        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");


        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema)
               .as("data")).select("data.*");

        Dataset<Row> maskeFilter = valueDS.filter(valueDS.col("search").equalTo("maske"));

        maskeFilter.writeStream().trigger(Trigger.ProcessingTime(60000))
                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                MongoSpark.write(rowDataset).option("collection","SearchMaske").mode("append").save();
            }
        }).start().awaitTermination();

        //valueDS.writeStream().format("console").outputMode("append").start().awaitTermination();
    }
}
