package StructuredStreaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
object ReadFromConsole {
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

    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .option("encoding", "UTF-8")
      .load()

    //Operasyon
    val words = lines.as[String].flatMap(_.split(" "))

    val wordcount = words.groupBy("value").count()
      .sort(desc("count"))


    //sonuc çıktı

    val query = wordcount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()


  }
}
