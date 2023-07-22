import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AppDiabetes {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("spark-mllib-naive-bayes").master("local").getOrCreate();

        Dataset<Row> loadData = sparkSession.read().format("csv").option("header", "true")
                .option("inferSchema", "true")
                .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\verisetleri\\diabetes.csv");

        String[] headerList = {"Pregnancies,","Glucose","BloodPressure",
                "SkinThickness","Insulin","BMI","DiabetesPedigreeFunction",
        "Age","Outcome"};

        List<String> arrayList = Arrays.asList(headerList);

        ArrayList<String> headersResult = new ArrayList<String>();

        for(String h: arrayList) {
        if(h.equals("Outcome"))
        {
            StringIndexer indexTemp = new StringIndexer().setInputCol(h).setOutputCol("label");
            loadData = indexTemp.fit(loadData).transform(loadData);
            headersResult.add("label");
        }
        else
        {
            StringIndexer indexTemp = new StringIndexer().setInputCol(h).setOutputCol(h.toLowerCase() +"_cat");
            loadData = indexTemp.fit(loadData).transform(loadData);
            headersResult.add(h.toLowerCase() +"_cat");
        }
        }
        String[] coList = headersResult.toArray(new String[headersResult.size()]);
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(coList).setOutputCol("features");
        Dataset<Row>transformData = vectorAssembler.transform(loadData);
        Dataset<Row> selectData = transformData.select("label", "features");
        selectData.show();
    }

    }

