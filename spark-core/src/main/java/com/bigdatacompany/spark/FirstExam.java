package com.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class FirstExam {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");
        JavaSparkContext javaSparkContext = new JavaSparkContext("local","FirstExamSpark");

        //Data Load
//        JavaRDD<String> firstdata =  javaSparkContext.textFile("C:\\Users\\umuto\\firstdata.txt");
//        System.out.println(firstdata.first());

        List<String> data = Arrays.asList("big data", "elasticsearch", "first", "spark", "hadoop");
        JavaRDD<String> secondData = javaSparkContext.parallelize(data);

        System.out.println(secondData.count());
        System.out.println(secondData.first());
    }
}
