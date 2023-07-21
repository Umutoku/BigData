package com.bigdatacompany.spark;

import com.bigdatacompany.spark.model.Person;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.awt.peer.SystemTrayPeer;
import java.util.Arrays;
import java.util.Iterator;

public class MapTrans {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");
        JavaSparkContext javaSparkContext = new JavaSparkContext("local","FirstExamSpark");

         JavaRDD<String> rawData = javaSparkContext.textFile("C:\\Users\\umuto\\OneDrive\\Masaüstü\\person.csv");

//         rawData.flatMap(new FlatMapFunction<String, String>() {
//             public Iterator<String> call(String s) throws Exception {
//                 return Arrays.asList(s.split(",")).iterator();
//             }
//         });

         JavaRDD<Person> loadPerson = rawData.map(new Function<String, Person>(){
             public Person call(String line) throws Exception{
                 String[] data = line.split(",");
                 Person p = new Person();
                 p.setFirst_name(data[0]);
                 p.setLast_name(data[1]);
                 p.setEmail(data[2]);
                 p.setGender(data[3]);
                 p.setCountry(data[4]);
                 return p;
             }
         });

//        JavaPairRDD<String, String> pairRDD = loadPerson.mapToPair(new PairFunction<Person, String, String>() {
//            public Tuple2<String, String> call(Person person) throws Exception {
//                return new Tuple2<String, String>(person.getEmail(), person.getCountry());
//            }
//        });
//
//        pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
//            public void call(Tuple2<String, String> data) throws Exception {
//                System.out.println("Key: " + data._1+ " -- Value: " +data._2);
//            }
//        });

        JavaPairRDD<String, Person> pairRDD = loadPerson.mapToPair(new PairFunction<Person, String, Person>() {
            public Tuple2<String, Person> call(Person person) throws Exception {
                return new Tuple2<String, Person>(person.getEmail(), person);
            }
        });

        JavaPairRDD<String, Iterable<Person>> groupData = pairRDD.groupByKey();

        groupData.foreach(new VoidFunction<Tuple2<String, Iterable<Person>>>() {
            public void call(Tuple2<String, Iterable<Person>> data) throws Exception {
                    System.out.println("Key: " +data._1 + " Value: "+ data._2);
            }
        });


/*
         loadPerson.foreach(new VoidFunction<Person>() {
             public void call(Person person) throws Exception {
                 System.out.println("Adı: "+ person.getFirst_name()+" Soyadı: "+person.getLast_name());
             }
         });
        JavaRDD<Person> personFromCanada = loadPerson.filter(new Function<Person, Boolean>() {
            public Boolean call(Person person) throws Exception {
                return person.getCountry().equals("Canada");
            }
        });

        personFromCanada.foreach(new VoidFunction<Person>() {
            public void call(Person person) throws Exception {
                System.out.println(person.getFirst_name() + "Üklesi: " + person.getCountry());
            }
        });
*/


    }
}
