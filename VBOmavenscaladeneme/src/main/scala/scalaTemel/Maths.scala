package scalaTemel

import scala.math._


object Maths {
  def main(args: Array[String]): Unit = {


    var myNumber = 1000
    myNumber +=1
    myNumber -=1

    println("myNumber 2 ile çarpıldı: "+ math.abs(-8))
    println("2nin 8inci kuvveti: "+ pow(2,8))
    println("2nin karekökü: "+ sqrt(8))
    println(exp(1))

    println("Round: "+ round(2.54)) // yuvarlama
    println("floor:" + floor(2.64)) //aşağı çeker
    println("ceil: "+ ceil(2.04)) // yukarı çeker

    println("Logaritma: " + log(2))

    println("2 ve 5 arası: " + min(2,5))
    println("2 ve 5 arası: " + max(2,5))
  }

}
