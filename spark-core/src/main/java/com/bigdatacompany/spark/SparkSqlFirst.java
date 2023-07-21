package com.bigdatacompany.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkSqlFirst {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType()
                .add("first_name", DataTypes.StringType)
                .add("last_name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("gender", DataTypes.StringType)
                .add("country", DataTypes.StringType);


        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();


        Dataset<Row> rawDS = sparkSession.read().option("header",true)
                .schema(schema).csv("C:\\Users\\umuto\\OneDrive\\Masaüstü\\person.csv");

//        rawDS.show();
//
//        rawDS.printSchema();

        Dataset<Row> chinaDS = rawDS.filter(rawDS.col("country").equalTo("China")
                        .and(rawDS.col("email").contains("google")))
                .sort("first_name");

        //chinaDS.show();

        //Dataset<Row> wcDS = rawDS.withColumn("firstNameTest", rawDS.col("first_name"));


        Dataset<Row> selectDS = rawDS.select("first_name", "last_name");
        //selectDS.show();

//        Dataset<Row> frbrDS = rawDS.filter("country = 'France' or country = 'Brazil'");

        Dataset<Row> countryGroupDS = rawDS.groupBy("first_name", "country").count();

        countryGroupDS.show();
    }
}
