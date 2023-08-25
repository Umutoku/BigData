package sparktemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapTransformation {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sparkTemelRDD")

    val sc = new SparkContext(conf)

    println("Metin dosyasından RDD oluşturma: ")
    val retailRDD = sc.textFile(path = "C:\\Users\\umuto\\OneDrive\\Masaüstü\\OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo"))

    println(retailRDD.first())

    case class CanceledPrice(isCancelled:Boolean,total:Double)
    val retailTotal = retailRDD.map(x=>{
      var isCanceled:Boolean = if (x.split(";")(0).startsWith("C")) true else false
      var total:Double = x.split(";")(3).toDouble * x.split(";")(5).replace(",",".").toDouble

      CanceledPrice(isCancelled = isCanceled,total = total)
    })

    //retailTotal.take(5).foreach(println)

    retailTotal.map(x=>(x.isCancelled,x.total))
      .reduceByKey((x,y)=>x+y)
      .filter(x=>x._1 == true)
      .take(5).foreach(println)
  }

}
