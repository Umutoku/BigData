package Streaming
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger, Level}
object WindowOps {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // sparkcon
    val conf = new SparkConf().setMaster("local[4]")
      .setAppName("StreamingWindowOps")

    // StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("D:\\datasets\\checkpoint")

    val sc = ssc.sparkContext

    val lines = ssc.textFileStream("D:\\datasets")

    ////////////Operasyon //////////////////

    val words = lines.flatMap(_.split(" "))

    val mappedWords = words.map(x=>(x,1))

    /////window operasyonu

    val window = mappedWords.window(Seconds(30),Seconds(10))

    ///2. CountByWindows

    val countByWindow = mappedWords.countByWindow(Seconds(30),Seconds(10))

    ///3. ReduceByKeyAndWindows

//    val reduceByKeyWindow = mappedWords
//      .reduceByKeyAndWindow((x,y)=>((x+y)),Seconds(30),Seconds(10))
//      .map(x=>(x._2,x._1))
//      .transform(_.sortByKey(false))


    ///////Yazdırma
    countByWindow.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
