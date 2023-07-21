package com.bigdatacompany.sparkhdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HdfsReader {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder().master("local").appName("Hdfs Reader").getOrCreate();

        Dataset<Row> rawDataCSV = sparkSession.read().option("header", true)
                .csv("hdfs://localhost:8020/data/movies.csv");

        rawDataCSV.show();

        Dataset<Row> milenyumOs = rawDataCSV.filter(rawDataCSV.col("title").contains("(2000)"));

        milenyumOs.show();

        milenyumOs.write().csv("hdfs://localhost:8020/data/milenyum");//hdfs içine yeni csv yaratıyor

        Dataset<Row> rawData = sparkSession.read().option("header", true)
                .csv("hdfs://localhost:8020/data/rating.csv");

        Dataset<Row> userId = rawData.groupBy("UserId").count();
        userId.show();

        //coalesce kaç parçaya böleceğini seçebilir.
        //partitionBy ile parçaları neye göre böleceğimize karar veririz
        userId.coalesce(1).write().partitionBy("userId")
                .csv("hdfs://localhost:8020/data/useridcount");


    }
}
