package sparktemel.DataFrameDataset
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{WriteConcernConfig, WriteConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._
object WriteToMongoDb {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WriteToMongoDb")
      .master("local[4]")
      .config("spark.mongodb.input.uri","mongodb://192.168.99.102:27017/test.myCollection")
      .config("spark.mongodb.output.uri","mongodb://192.168.99.102:27017/test.myCollection")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema","true")
      .option("sep",",")
      .load("D:\\Datasets\\simple_data.csv")

    df.show()

  val writeConfig = WriteConfig(Map("collection"->"spark","writeConcern.w" -> "majority"),
  Some(WriteConfig(sc)))

    MongoSpark.save(df,writeConfig)

  }
}
