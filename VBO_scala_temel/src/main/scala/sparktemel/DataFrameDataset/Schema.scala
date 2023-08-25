package sparktemel.DataFrameDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Schema {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Schema")
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

    df.printSchema()

    //Spark şemamızı kendimiz yapalım

    val retailManualSchema = new StructType(
      Array(
      new StructField("InvoiceNo",StringType,true),
      new StructField("StockCode",StringType,true),
      new StructField("Description",StringType,true),
      new StructField("Quantity",IntegerType,true),
      new StructField("InvoiceDate",StringType,true),
      new StructField("UnitPrice",FloatType,true),
      new StructField("CustomerID",IntegerType,true),
      new StructField("Country",StringType,true)

    ))

    val df2 = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .schema(retailManualSchema)
      .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\OnlineRetail.csv")

    df2.printSchema()

    val df3 = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .schema(retailManualSchema)
      .load("C:\\Users\\umuto\\OneDrive\\Masaüstü\\OnlineRetail.csv")
      .withColumn("UnitPrice",regexp_replace($"UnitPrice",",","."))

    df3.show(10)
  }



}
