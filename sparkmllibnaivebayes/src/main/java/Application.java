import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("spark-mllib-naive-bayes").master("local").getOrCreate();

        Dataset<Row> loadData = sparkSession.read().format("csv").option("header", "true")
                .option("inferSchema", "true")
                .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\verisetleri\\basketbol.csv");

        StringIndexer indexHava = new StringIndexer().setInputCol("hava").setOutputCol("hava_cat");
        StringIndexer indexNem = new StringIndexer().setInputCol("nem").setOutputCol("nem_cat");
        StringIndexer indexSicaklik = new StringIndexer().setInputCol("sicaklik").setOutputCol("sicaklik_cat");
        StringIndexer indexRuzgar = new StringIndexer().setInputCol("ruzgar").setOutputCol("ruzgar_cat");
        StringIndexer indexLabel = new StringIndexer().setInputCol("basketbol").setOutputCol("label");

        Dataset<Row> transformHava = indexHava.fit(loadData).transform(loadData);
        Dataset<Row> transformNem = indexNem.fit(transformHava).transform(transformHava);
        Dataset<Row> transformSicaklik = indexSicaklik.fit(transformNem).transform(transformNem);
        Dataset<Row> transformRuzgar = indexRuzgar.fit(transformSicaklik).transform(transformSicaklik);
        Dataset<Row> transformLabel = indexLabel.fit(transformRuzgar).transform(transformRuzgar);

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"hava_cat","nem_cat","sicaklik_cat","ruzgar_cat","label"})
                        .setOutputCol("features");

        Dataset<Row> transform = vectorAssembler.transform(transformLabel);

        Dataset<Row> finalData = transform.select("label", "features");

        //finalData.show();

        Dataset<Row>[] datasets = finalData.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainData = datasets[0];
        Dataset<Row> testData = datasets[1];

        NaiveBayes nb = new NaiveBayes();
        nb.setSmoothing(1);
        NaiveBayesModel model = nb.fit(trainData);
        Dataset<Row> tahminData = model.transform(testData);

        //tahminData.show();

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuary");

        double evaluate = evaluator.evaluate(tahminData);

    }
}
