package MachineLearning.Preprocessing
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.StringIndexer
object PreprocessOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadFromMongoDb")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load("D:\\Datasets\\simple_data.csv")

    println("Orjinal DF")
    df.show()


    val df1 = df.withColumn("ekonomik_durum",
      when(col("aylik_gelir").gt(7000),"iyi")
      .otherwise("kötü")
    )
    println("Ekonomik durum eklenmiş DF")
    df1.show(15)

    ////////////////// StringIndexer Aşaması ///////////////////
    //----------------------------------------

    val meslekIndexer = new StringIndexer()
      .setInputCol("meslek")
      .setOutputCol("meslekIndex")
      .setHandleInvalid("skip")

    val meslekIndexerModel = meslekIndexer.fit(df1)

    val meslekIndexedDF = meslekIndexerModel.transform(df1)

    println("meslek sütunu için StringIndex eklenmiş DF ")
    meslekIndexedDF.show()

    println("Meslek frekansları: ")
    df.groupBy(col("meslek"))
      .agg(
        count(col("*")).as("sayi")
      )
      .sort(desc("sayi"),asc("meslek"))
      .show()

    val sehirIndexer = new StringIndexer()
      .setInputCol("sehir")
      .setOutputCol("sehirIndexer")
      .setHandleInvalid("skip")

    val sehirIndexerIndexModel = sehirIndexer.fit(meslekIndexedDF)
    val sehirIndexerDF = sehirIndexerIndexModel.transform(meslekIndexedDF)

    println("sehir sütunu için StringIndex eklenmiş DF ")
    sehirIndexerDF.show()

    println("Sehir frekansları: ")
    df.groupBy(col("sehir"))
      .agg(
        count(col("*")).as("sayi")
      )
      .sort(desc("sayi"), asc("sehir"))
      .show()

  }

}
