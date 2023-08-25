package sparktemel.DataFrameDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrameGiris {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataFrameGiris")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    //Listeden
    val dfFromList = sc.parallelize(List(1,2,3,4,5,6,3,4,5)).toDF("rakamlar")
    //dfFromList.printSchema()

    //Range ile
    val dfFromSpark = spark.range(10,100,5).toDF("besli")
    //dfFromSpark.printSchema()

    //Dosyadan okuyarak
    val dfFromFile = spark.read.format("csv")
      .option("sep",";")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\OnlineRetail.csv")
    //dfFromFile.printSchema()
    //dfFromFile.show(10,false)

    //*********** dataframe örnek transformation ve action ************
    //println("\nOnlineRetail satır sayısı: " + dfFromFile.count())

    //dfFromFile.select("Quantity","InvoiceNo").show()

    dfFromFile.sort(dfFromFile.col("Quantity")).show(10)

    spark.conf.set("spark.sql.shuffle.partitions","5")

    dfFromFile.sort($"Quantity").explain()



  }

}
