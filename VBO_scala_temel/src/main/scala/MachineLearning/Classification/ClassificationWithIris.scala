package MachineLearning.Classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ClassificationWithIris {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ClassificationWithIris")
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
      .load("D:\\Datasets\\iris.csv").cache()

    df.show()

    df.describe().show()

    df.groupBy("Species").count().show()

    /////////////////// Veri temizliği ve ön hazırlığı ////////////
    //2.1 Inputer -> sayısal değerlerdeki boşlukları doldurma

    // Boş nitelik olmadığı için kullanılmadı

    // 2.2 kategorik Nitelikler için stringindexer ve onehotencoder

    //Kategorik nitelik yok
    //sadece hedef değişken için gerekecek

    import org.apache.spark.ml.feature.StringIndexer

    val stringIndexer = new StringIndexer()
      .setInputCol("Species")
      .setOutputCol("label")
      .setHandleInvalid("skip")

    val labelDF = stringIndexer.fit(df).transform(df)
    println("LabelDF\n")
    labelDF.show()

    //2.3 VectorAssembler aşaması

    import org.apache.spark.ml.feature.VectorAssembler

    val assembler = new VectorAssembler()
      .setInputCols(Array("SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm"))
      .setOutputCol("features")

    val vectorDF = assembler.transform(labelDF)
    println("VectorDF\n")
    vectorDF.show()

    // 2.4 Normalizasyon

    //Nitelikler aynı ölçekte olduğu için kullanmıyoruz

    // 2.6 veriyi traindf ve testdf olarak ayırma

    val Array(trainDF,testDF) = vectorDF.randomSplit(Array(0.8,0.2),seed = 142L)

    ///////// 3 Model oluşturma aşaması
    //Sınıflandırıcı nesnesi yaratma

    import org.apache.spark.ml.classification.LogisticRegression

    val logregObj = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")

    // 3.2 sınıflandırıcı nesne ver ile eğitme ve model elde etme

    val logregModel = logregObj.fit(trainDF)

    // 3.3 Modeli kullanarak tahmin yapma

    val transformedDF = logregModel.transform(testDF)

    println("transformedDF\n")
    transformedDF.show()

    // Model değerlendirme
    //4.1 değerlendirici kütüphane indirme ve nesne oluşturma

    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val accuracy = evaluator.evaluate(transformedDF)

    println("Accuracy", accuracy)







  }

}
