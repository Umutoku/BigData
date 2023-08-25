package sparktemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapFlatMap {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("map ve flatmap").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //RDD okuma
    var rddFromTextFile  = sc.textFile(path = "C:\\Users\\umuto\\OneDrive\\Masaüstü\\simple_data.csv")
      .filter(!_.contains("sirano"))

    rddFromTextFile.take(3).foreach(println)

    rddFromTextFile.map(x=>x.toUpperCase).take(5).foreach(println)
    rddFromTextFile.flatMap(x=>x.split(",")).map(x=>x.toUpperCase).take(5).foreach(println)
  }

}
