package com.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class ActionApp {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");
        JavaSparkContext javaSparkContext = new JavaSparkContext("local","FirstExamSpark");

        JavaRDD<String> rawData = javaSparkContext.textFile("C:\\Users\\umuto\\OneDrive\\Masaüstü\\person.csv");

        System.out.println(rawData.count());
        System.out.println(rawData.take(3)); // içinden 3 adet eleman verecek
        rawData.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
