package sparktemel.DataFrameDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._
object ReadFromPostgres {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadFromPostgres")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df = spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url","jdbc:postgresql://docker:5432/postgres")
      .option("dbtable","simple_data")
      .option("user","postgres")
      .option("password","postgres")
      .load()

    df.show(10)




  }
}
