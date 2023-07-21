import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.sql.Struct;

public class IoTWeather {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder().master("local")
                .appName("SparkStreamingListener").getOrCreate();

        StructType weatherType = new StructType()
                .add("quarter", "string")
                .add("heatType","string")
                .add("heat","integer")
                .add("windType","string")
                .add("wind", "integer");

        Dataset<Row> rawData = sparkSession.readStream().schema(weatherType).option("sep", ",")
                .csv("C:\\Users\\umuto\\OneDrive\\Masaüstü\\BuyukVeriyeGiris_Dokumanlar\\sparkstreaming\\*");


        Dataset<Row> heatData = rawData.select("quarter", "heat", "wind").where("wind > 25 AND heat >30");

        StreamingQuery start = heatData.writeStream().format("console").start();

        start.awaitTermination();

    }
}
