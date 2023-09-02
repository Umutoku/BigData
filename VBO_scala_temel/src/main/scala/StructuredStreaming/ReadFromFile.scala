package StructuredStreaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object ReadFromFile {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadFromFile")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    //Satırları oku

    val lines = spark.readStream
      .format("csv")
      .load("D:\\Datasets")

    //Transformasyon

    val words = lines.as[String].flatMap(x=>x.split(" "))

    // aggregation words

    val wordCounts = words.groupBy("value").count()
      .sort(desc("count"))


    //Sonuc


    //Streaming start
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
