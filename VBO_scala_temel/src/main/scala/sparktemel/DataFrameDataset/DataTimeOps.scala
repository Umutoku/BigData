package sparktemel.DataFrameDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._
object DataTimeOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataTimeOps")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\OnlineRetail.csv")
      .select("InvoiceDate").distinct()

    //df.show()

    //Varsayılan format : yyyy-MM-dd HH:mm:ss

    val mevcutFormat = "dd.MM.yyyy HH:mm"

    val df2 = df.withColumn("InvoiceDate",F.trim($"InvoiceDate")) //boşul temizle
      .withColumn("NormalTarih", F.to_date($"InvoiceDate",mevcutFormat)) //normal formatta tarih
      .withColumn("StandartTS",F.to_timestamp($"InvoiceDate",mevcutFormat)) // normal timestamp
      .withColumn("UnixTimestamp",F.unix_timestamp($"StandartTS"))
      .withColumn("BirYıl",F.date_add($"StandartTS",365))
      .withColumn("Year",F.year($"StandartTS"))
      .withColumn("Fark",F.datediff($"BirYıl",$"StandartTS"))

    df2.show()


  }

}
