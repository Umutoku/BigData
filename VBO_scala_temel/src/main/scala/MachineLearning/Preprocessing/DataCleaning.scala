package MachineLearning.Preprocessing
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object DataCleaning {
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

    val adultWholeDF = adultTrainDF.union(adultTestDF)

    adultWholeDF.show(10)

    //"""""""""""""""""""""""VERİ TEMİZLİĞİ BAŞLIYOR""""""""""""""""""""""""""

    val adultWholeDFI = adultWholeDF
      .withColumn("workclass",trim(col("workclass")))
      .withColumn("education",trim(col("education")))
      .withColumn("marital_status",trim(col("marital_status")))
      .withColumn("occupation",trim(col("occupation")))
      .withColumn("relationship",trim(col("relationship")))
      .withColumn("race",trim(col("race")))
      .withColumn("sex",trim(col("sex")))
      .withColumn("native_country",trim(col("native_country")))
      .withColumn("output",trim(col("output")))
    //""""""""""""""""""" nokta temizliği
      .withColumn("output",regexp_replace(col("output"),"<=50K.","<=50K"))
      .withColumn("output",regexp_replace(col("output"),">50K.",">50K"))
      //Okulları birleştirme """"""""""""""""""""""""""""""""""""""
      .withColumn("education_merged",
        when(col("education")
          .isin("1st-4th","5th-6th","7th-8th"),"Elemantry-School")
      .otherwise(col("education")))



  //"""""""""""""""""""""""" SORU İŞARETİ ? KONTROLÜ"""""""""""""""""""""""""
//    var sayacForQuestion = 1
//    var sutunList  = List[String]()
//
//    for(sutun <- adultWholeDFI.columns){
//      if(adultWholeDFI.filter(col(sutun).contains("?")).count()>0){
//        println(sayacForQuestion+ ". " + sutun  + " içinde ? var")
//        sutunList = sutun :: sutunList
//      }else{
//        println(sayacForQuestion + ". " + sutun)
//      }
//      sayacForQuestion +=1
//    }
//
//
//    sutunList.foreach(println)
    val adultWholeClear = adultWholeDFI
      .filter(!(col("workclass").contains("?") ||
        col("occupation").contains("?") ||
        col("native_country").contains("?") ||
        col("output").contains("?") ||
        col("education").contains("?") ||
        col("marital_status").contains("?") ||
        col("relationship").contains("?") ||
        col("race").contains("?") ||
        col("sex").contains("?")))

    adultWholeClear.show(20)

    val nitelikSiralama = Array[String]("workclass", "education", "education_merged", "marital_status", "occupation", "relationship", "race",
      "sex", "native_country", "age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week", "output")

    val adultWhole = adultWholeClear.select(nitelikSiralama.head,nitelikSiralama.tail:_*)

    //diske yazma """"""""""""""""""
    adultWhole
      .coalesce(1)
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header","true")
      .csv("D:\\Datasets\\adultpreprocessed")

  }

}
