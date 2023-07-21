import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Random;

public class Application {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("spark-mllib").master("local").getOrCreate();

        Dataset<Row> rawData = sparkSession.read().format("csv").option("header", "true")
                .option("inferSchema", "true")
                .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\verisetleri\\satis.csv");

        //rawData.show();

        VectorAssembler features_vector = new VectorAssembler().setInputCols(new String[]{"Ay"})
                .setOutputCol("features");

        Dataset<Row> transform = features_vector.transform(rawData);

        //transform.show();

        Dataset finalData = transform.select("features", "Satis");

        double rangeMin = 0.1;
        double rangeMax = 0.9;
        Random r = new Random();
        double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
        double[] doubles = {0.5, 0.5};
        Dataset<Row>[] datasets = finalData.randomSplit(doubles);



        Dataset<Row> trainData = datasets[0];
        Dataset<Row> testData = datasets[1];

        LinearRegression lr = new LinearRegression();
        lr.setLabelCol("Satis");

        LinearRegressionModel model = lr.fit(trainData);

        LinearRegressionTrainingSummary summary = model.summary();

        if(summary.r2()<0.98) {
            for(int i = 0; i < 10; i++) {
                doubles[0] = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
                doubles[1] = 1 - doubles[0];
                datasets = finalData.randomSplit(doubles);
                trainData = datasets[0];
                lr = new LinearRegression();
                lr.setLabelCol("Satis");
                model = lr.fit(trainData);
                summary = model.summary();
                System.out.println("Sonuç");
                System.out.println("------------");
                System.out.println(doubles[0]);
                System.out.println(doubles[1]);
                System.out.println("------------");
                System.out.println(summary.r2());
            }
        }

//        System.out.println(doubles[0]);
//        System.out.println(doubles[1]);


//        Dataset<Row> transformTest = model.transform(testData);
//
//        transformTest.show();
    }
}
