package com.veribilimiokulu
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
object SparkDeneme {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
  println("Merhaba Spark")

    val sc = new SparkContext("local[4]","SparkDeneme")

    val myRDD = sc.parallelize(List(1,2,3,4,5,6,7,8))

    myRDD.take(8).foreach(println)
  }

}
