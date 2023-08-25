package sparktemel.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import sparktemel.RDD.PairRDDOps.Printer
object Join {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sparkTemelRDD")

    val sc = new SparkContext(conf)

    val productsRDD = sc.textFile(path = "C:\\Users\\umuto\\OneDrive\\Masaüstü\\products.csv")
      .filter(!_.contains("productDescription"))

    val orderItemRDD = sc.textFile(path = "C:\\Users\\umuto\\OneDrive\\Masaüstü\\order_items.csv")
      .filter(!_.contains("orderItemName"))

    println("\n products ve order ilk göz atma")
    orderItemRDD.print
    productsRDD.print

    def makeOrderItemsPairRDD(line:String) = {
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2)
      val orderItemQuantity = line.split(",")(3)
      val orderItemSubTotal = line.split(",")(4)
      val orderItemProductPrice = line.split(",")(4)

      (orderItemProductId,(orderItemName,orderItemOrderId,orderItemProductId,orderItemQuantity,orderItemSubTotal,orderItemProductPrice))
    }

    val orderItemsPairRDD = orderItemRDD.map(makeOrderItemsPairRDD)
    orderItemsPairRDD.print

    def makeProductsPairRDD(line: String) = {
      val productId = line.split(",")(0)
      val productCategoryId = line.split(",")(1)
      val productName = line.split(",")(2)
      val productDescription = line.split(",")(3)
      val productPrice = line.split(",")(4)
      val productImage = line.split(",")(5)

      (productId, (productCategoryId, productName, productDescription, productPrice, productImage))
    }


    val productsPairRDD = productsRDD.map(makeProductsPairRDD)

    productsPairRDD.print

    val orderItemProductJoinedRDD = orderItemsPairRDD.join(productsPairRDD)
    orderItemProductJoinedRDD.print



  }
}
