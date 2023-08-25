package sparktemel.DataFrameDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
object StringOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("StringOps")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val dfFromFile = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\simple_dirty_data.csv")

    dfFromFile.show(10)

    //1. Concat

    val df2 = dfFromFile.select("meslek","sehir")
      .withColumn("meslek_sehir",concat(col("meslek"),lit(" - ")
        ,col("sehir")))
      .show(10,truncate = false)

  }

}
