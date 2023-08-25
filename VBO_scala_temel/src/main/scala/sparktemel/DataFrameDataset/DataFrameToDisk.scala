package sparktemel.DataFrameDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
object DataFrameToDisk {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Schmema")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df= spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\simple_dirty_data.csv")

    df.show(10)

   val df2=  df
      .withColumn("isim",trim(initcap($"isim")))
      .withColumn("cinsiyet",when($"cinsiyet".isNull,"U").otherwise($"cinsiyet"))
      .withColumn("sehir",
        when($"sehir".isNull,"BİLİNMİYOR")
      .otherwise($"sehir"))
      .withColumn("sehir",trim(upper($"sehir")))

    df2.show(10)

    df2.coalesce(1)
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header","true")
      .csv("C:\\Users\\umuto\\OneDrive\\Masaüstü\\simple_dirty_data")


  }

}
