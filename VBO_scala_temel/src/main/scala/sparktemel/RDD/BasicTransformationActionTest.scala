package sparktemel.RDD
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j.{Logger,Level}
object BasicTransformationActionTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[2]")
      .set("spark.driver.memory","2g")
      .setExecutorEnv("spark.executor.memory","3g")

    val sc = new SparkContext(sparkConf)
    println("Merhaba Spark")

    //Soru 2

    val rddRakam = sc.parallelize(List(3,7,13,15,22,36,7,11,3,25))
    println("Soru 2")
    rddRakam.foreach(println)

    //Soru 3
    val metinRDD = sc.makeRDD(List("Spark'ı öğrenmek çok heyecan verici"))
    println("Soru 3")
    metinRDD.map(x=>x.toUpperCase).foreach(println)

    //Soru 4
    val rddFromTextFile  = sc.textFile(path = "C:\\Users\\umuto\\OneDrive\\Masaüstü\\Ubuntu_Spark_Kurulumu.txt")
    println(rddFromTextFile.count())

    println("Soru 5")
    val kelime =rddFromTextFile.flatMap(x => x.split(" ")).count()

    println(kelime)

    println("Soru 6")

    val rddRakam2 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    rddRakam.intersection(rddRakam2).collect().foreach(println)
    println("Soru 7")

    val rakamTekil = List(3,7,13,15,22,36,7,11,3,25).distinct
    val rakamTekilRDD = sc.parallelize(rakamTekil)
    rakamTekilRDD.collect().foreach(println)

    println("Soru 7")
    val rakam = List(3,7,13,15,22,36,7,11,3,25)
    val rakamRDD = sc.parallelize(rakam)
    rakamRDD.map(x=>(x,1)).reduceByKey((x,y)=>x+y).collect().foreach(println)

  }

}
