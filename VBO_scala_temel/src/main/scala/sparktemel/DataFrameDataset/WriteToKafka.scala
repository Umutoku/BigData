package sparktemel.DataFrameDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._
object WriteToKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WriteToKafka")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header", "true")
      .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\Advertising.csv")

    df.show(2)

    val df2= df.withColumn("key",F.col("ID")).drop("ID")
      .withColumn("value",F.concat(
        F.col("TV"),F.lit(","),
        F.col("Radio"),F.lit(","),
        F.col("Newspaper"),F.lit(","),
        F.col("Sales"),F.lit(",")
      )).drop("TV","Radio","Newspaper","Sales")

    df2.show(5)

    df2.write
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","deneme")
      .save()
  }

}
