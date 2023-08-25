package sparktemel.DataFrameDataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Wordcount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Wordcount")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    //  val sc = spark.sparkContext

    import spark.implicits._

    val hikayeDS = spark
      .read.textFile("C:\\Users\\umuto\\OneDrive\\Masaüstü\\omer_seyfettin_forsa_hikaye.txt")
    val hikayeDF = hikayeDS.toDF("value")
    hikayeDF.show(10,truncate = false)

    val kelimeler = hikayeDS.flatMap(x=>x.split(" "))
    println(kelimeler.count())

    kelimeler.show(5)

    import org.apache.spark.sql.functions.{count}
    kelimeler.groupBy("value")
      .agg(count('value).as("kelimeSayisi"))
      .orderBy($"kelimeSayisi".desc)
      .show(10)


  }


}
