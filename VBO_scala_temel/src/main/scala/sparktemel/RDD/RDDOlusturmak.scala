package sparktemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object RDDOlusturmak {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

//    val spark = SparkSession.builder()
//      .master("local[4]")
//      .appName("RDD-Olusturma")
//      .config("spark.driver.memory","2g")
//      .config("spark.executer.memory","4g")
//      .getOrCreate()
//
//    val sc = spark.sparkContext

    val conf = new SparkConf()
      .setMaster("Local[4]")
      .setAppName("RDD-Olusturma")
      .setExecutorEnv("spark.executer.memory","4g")

    val sc = new SparkContext(conf)

    println("List'den RDD oluşturma: ")
    val rddFromList = sc.makeRDD(List(1,2,3,4,5,6,7,8)) // = ParallelCollectionRDD[0] at makeRDD at
    rddFromList.take(4).foreach(println)

    println("Tuple'dan RDD oluşturma: ")
    val rddFromListTuple = sc.makeRDD(List((1,2,3),(4,5),(6,7,8))) // [Product with Serializable] = ParallelCollectionRDD[1]
    rddFromListTuple.take(3).foreach(println)

    println("Metin dosyasından RDD oluşturma: ")
    var rddFromTextFile  = sc.textFile(path = "C:\\Users\\umuto\\OneDrive\\Masaüstü\\OnlineRetail.csv")
    rddFromTextFile.take(4).foreach(println)

  }

}
