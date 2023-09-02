package StructuredStreaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.StringType
object ReadFromKafka {
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

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "deneme")
      .option("encoding", "UTF-8")
      .load()


    val df2 = df.select($"key".cast(StringType), $"value".cast(StringType))

    val df3 = df2.select($"value").as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value").count()
      .sort(functions.desc("count"))

    val query = df3.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}
