package DistributedVariables

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import sparktemel.RDD.PairRDDOps.Printer

import scala.collection.immutable.Map
import scala.io.Source
object BroadcastVariablesOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("BroadcastVariablesOps")

    val sc = new SparkContext(conf)

    // Ürünler okuma

    def load_products():Map[Int,String]={
      val source = Source.fromFile("C:\\Users\\umuto\\OneDrive\\Masaüstü\\products.csv")
      val lines = source.getLines()
        .filter(x=>(!(x.contains("productCategoryId"))))

      var productIdandName: Map[Int,String] = Map()
      for (line <- lines){
        val productId = line.split(",")(0).toInt
        val productName = line.split(",")(2)
        productIdandName += (productId-> productName)
      }
      return productIdandName
    }

    val broadcastProduct = sc.broadcast(load_products)

    //Siparişler okuma

    val orderItemsRDD = sc.textFile("C:\\Users\\umuto\\OneDrive\\Masaüstü\\order_items.csv")
      .filter(!_.contains("orderItemName"))


    def makeOrderItemsPairRDD(line:String)={
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2).toInt
      val orderItemQuantity = line.split(",")(3)
      val orderItemSubTotal = line.split(",")(4).toFloat
      val orderItemProductPrice = line.split(",")(5)

      (orderItemProductId,orderItemSubTotal)
    }

    val orderItemsPairRDD = orderItemsRDD.map(makeOrderItemsPairRDD)

    orderItemsPairRDD.print

    val sortedOrderItemsPairRDD = orderItemsPairRDD.reduceByKey((x,y)=>x+y)
      .map(x=> (x._2,x._1))
      .sortByKey(false)
      .map(x=> (x._2,x._1))


    val sortedOrdersWithProductName = sortedOrderItemsPairRDD
      .map(x=> (broadcastProduct.value(x._1),x._2))

    sortedOrdersWithProductName.print

  }

}
