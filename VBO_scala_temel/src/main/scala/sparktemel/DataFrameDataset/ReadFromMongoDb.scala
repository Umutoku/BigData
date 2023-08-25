package sparktemel.DataFrameDataset
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConcernConfig, WriteConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._
object ReadFromMongoDb {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadFromMongoDb")
      .master("local[4]")
      .config("spark.mongodb.input.uri", "mongodb://192.168.99.102:27017/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://192.168.99.102:27017/test.myCollection")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    //Read from mongodb

    val readConfig = ReadConfig(Map("collection"-> "spark","readPreference.name"->"secondaryPreferred"),
      Some(ReadConfig(sc)))

    val simple_dataRDD = MongoSpark.load(sc,readConfig)

    val df = simple_dataRDD.toDF()

    df.show()

  }
}
