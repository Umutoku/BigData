package MachineLearning.Preprocessing
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConcernConfig, WriteConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._
object DataExplore {
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

    val adultTrainDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load("D:\\Datasets\\adult.data")

    val adultTestDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load("D:\\Datasets\\adult.test")

    println("\n Train Data")
    //adultTrainDF.show(5)

    println("\n Test Data")
    //adultTestDF.show(5)

    val adultWholeDF = adultTrainDF.union(adultTestDF)

    //adultWholeDF.show(5)

    //""""""""""""""""" Veri seti ve şema karşılaştırma """""""""""""""""""""""""""""""

    adultWholeDF.printSchema()

    println("Numerik değişkenlerin istatistiklerini görelim")
    adultWholeDF.describe("age","fnlwgt","education_num","capital_gain","capital_loss","hours_per_week")
      .show()

    println("workclass groupby inceleme")
    adultWholeDF.groupBy($"workclass")
      .agg(F.count($"*").as("sayi")).show()

    // soru işaretleri ve en alt iki değer çıkarılabilir

    println("education groupby inceleme")
    adultWholeDF.groupBy($"education")
      .agg(F.count($"*").as("sayi")).show()

    //Çok fazla kategori var, birleştirilebilir
    //lise üniversite, yüksek, doktora şeklinde

    println("marital groupby inceleme")
    adultWholeDF.groupBy($"marital_status")
      .agg(F.count($"*").as("sayi")).show()

    //Evlilik durumlarında bir sorun gözükmüyor

    println("occupation groupby inceleme")
    adultWholeDF.groupBy($"occupation")
      .agg(F.count($"*").as("sayi")).show()

    //Armed çok az bunlar çıkarılabilir, ? olanlar çıkarılmalı


    println("relationship groupby inceleme")
    adultWholeDF.groupBy($"relationship")
      .agg(F.count($"*").as("sayi")).show()

    //sorun gözükmüyor

    println("race groupby inceleme")
    adultWholeDF.groupBy($"race")
      .agg(F.count($"*").as("sayi")).show()

    //sorun gözükmüyor, çok fazla white var

    println("sex groupby inceleme")
    adultWholeDF.groupBy($"sex")
      .agg(F.count($"*").as("sayi")).show()

    //üçte biri kadın gerisi erkek

    println("native_country groupby inceleme")
    adultWholeDF.groupBy($"native_country")
      .agg(F.count($"*").as("sayi")).show()

    //Büyük çoğunluk USA'dan

    println("output groupby inceleme")
    adultWholeDF.groupBy($"output")
      .agg(F.count($"*").as("sayi")).show()

    // "," içeren değişkenler var, bunlar temizlenmeli



  }
}
