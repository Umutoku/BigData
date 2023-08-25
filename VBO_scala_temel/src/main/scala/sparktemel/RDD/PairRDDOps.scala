package sparktemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object PairRDDOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sparkTemelRDD")

    val sc = new SparkContext(conf)

    val retailRDD = sc.textFile(path = "C:\\Users\\umuto\\OneDrive\\Masaüstü\\simple_data.csv")
      .filter(!_.contains("sirano"))

    retailRDD.print


    def meslekMaasPair(line:String)={
      val meslek = line.split(",")(3)
      val maas = line.split(",")(5).toDouble

      (meslek,maas)
    }
    val meslekMaasRDD = retailRDD.map(meslekMaasPair)

    val meslegeGoreMaas = meslekMaasRDD.mapValues(x=>(x,1))

    println("Mesleğe göre maaş")
    meslegeGoreMaas.print

    val meslekMaasRBK = meslegeGoreMaas.reduceByKey((x,y)=>(x._1 + y._1,x._2 + y._2))
    println("Meslege göre reduce")
    meslekMaasRBK.print

    println( "\nMeslege göre ortalama")
    val meslekOrtalamaMaas = meslekMaasRBK.mapValues(x=>x._1/x._2)
    meslekOrtalamaMaas.print


  }

  implicit class Printer(rdd: org.apache.spark.rdd.RDD[_]) {
    def print = {
      println(s"Yeni işlem ")
      rdd.take(50).foreach(println)
    }
  }

}
