import org.apache.spark.sql.SparkSession;

public class AppDiabetes {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("spark-mllib-naive-bayes").master("local").getOrCreate();
    }
}
