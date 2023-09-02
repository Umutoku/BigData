package StructuredStreaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, desc}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
object ReadFromCsv {
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

    // Şema

    val mySchema = new StructType()
      .add("sirano", IntegerType)
      .add("isim", StringType)
      .add("yas", IntegerType)
      .add("meslek", StringType)
      .add("sehir", StringType)
      .add("aylik_gelir", DoubleType)



    ///Veri okuma

    val df = spark.readStream
      .format("csv")
      .option("header","true")
      .option("sep",",")
      .load("D:\\Datasets")

    //mesleklere göre ortalama gelir

    val meslekOrtalamaGelir = df.groupBy("meslek")
      .agg(avg("aylik_gelir").as("ortGelir"))
      .sort(desc("ortGelir"))


    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
