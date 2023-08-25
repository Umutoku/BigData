package scalaTemel

import breeze.stats._
import org.apache.spark.ml.linalg._
object VectorOps {
  def main(args: Array[String]): Unit = {

    val zeroVector = Vectors.dense(Array(0.0,0.0,0.0,0.0))

    val yasVector = Vectors.dense(Array(27.0,42.0,23.0,28.0)).toDense
    val yasVector2 = Vectors.dense(Array(27.0,32.0,24.0,27.0)).toDense

    println("yasVector ortalaması: "+ mean(yasVector.toArray))

    println("yasVector standart sapması: "+ stddev(yasVector.toArray))

    println("yasVector varyans: "+ variance(yasVector.toArray))

    println("yasVector arasındaki mesafe: "+ Vectors.sqdist(yasVector,zeroVector))

    def kovaryansHesapla(x:DenseVector,y:DenseVector):Double={
      var kovaryans:Double =0.0
      val xMean = mean(x.toArray)
      val yMean = mean(y.toArray)
      var total = 0.0
      val n = x.size.toDouble
      for(i<-0 until x.size){
        total += (((x(i)-xMean) * (y(i)-yMean)))
      }
      kovaryans = (1.0/n)*total
      kovaryans
    }

    def kolerasyonHesapla(x:DenseVector,y:DenseVector):Double={
      var kovaryans = kovaryansHesapla(x,y)
      val korelasyon = kovaryans/(stddev(x.toArray)*stddev(y.toArray))
      korelasyon
    }

    println("Kolerasyon: "+ kolerasyonHesapla(yasVector,yasVector2))
  }

}
