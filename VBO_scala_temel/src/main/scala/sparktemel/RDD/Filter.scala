package sparktemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sparkTemelRDD")

    val sc = new SparkContext(conf)

    println("Metin dosyasından RDD oluşturma: ")
    val retailRDDWithHeader = sc.textFile(path = "C:\\Users\\umuto\\OneDrive\\Masaüstü\\OnlineRetail.csv")

    val retailRDD = retailRDDWithHeader.mapPartitionsWithIndex(
      (idx,iter)=> if(idx==0) iter.drop(1) else iter
    )

    println("\n \n")
    println("********* Birim miktarı 30dan küçük olanları listele **********")

    retailRDD.filter(x=>x.split(";")(3).toInt<30)
      .take(5).foreach(println)

    println("\n \n")
    println("********* Birim fiyatı 20dan büyük ve içinde COFFEE olanları listele **********")

//    retailRDD.filter(x => x.split(";")(2)
//      .contains("COFFEE") &&  x.split(";")(5)
//      .trim.replace(",",".")
//      .toFloat > 20.0F)
//      .take(5).foreach(println)

    def coffeePrice20(line:String):Boolean={
      var sonuc = true
      var Description:String = line.split(";")(2)
      var UnitPrice:Float = line.split(";")(5)
        .trim.replace(",",".").toFloat
      sonuc = Description.contains("COFFEE") && UnitPrice >20.0
      sonuc
    }

    retailRDD.filter(x=>coffeePrice20(x)).take(5).foreach(println)


  }

}
