package sparktemel.DataFrameDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object CsvDosyasinaSQLAtmak {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Wordcount")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

     val sc = spark.sparkContext

    import spark.implicits._

    val dfFromFile = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .option("inferschema","true")
      .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\OnlineRetail.csv")

    dfFromFile.cache()

    dfFromFile.createOrReplaceTempView("tablo")

    spark.sql(
      """

        SELECT Country, SUM(Quantity) AS Quantity
        FROM tablo
        GROUP BY Country
        ORDER BY Quantity DESC


        """).show(20)

  println("\n İkinci sorgu \n")

    spark.sql(
      """

        SELECT Country, SUM(UnitPrice) AS UnitPrice
        FROM tablo
        GROUP BY Country
        ORDER BY UnitPrice DESC


        """).show(20)


  }



}
