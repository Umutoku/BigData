package scalaTemel

import scala.collection.mutable.ArrayBuffer

object Arrays {
  def main(args: Array[String]): Unit = {

    var rakamArray = new Array[Int](20)
    rakamArray(10) = 15

    rakamArray.foreach(println)

    var i = 0
    rakamArray.foreach(x=>{
      if(x.equals(15)){
        println("15 " + i + "nci indiste")
      } else {
        println("Burada değil")
      }
      i+=1
    })

    for(i<-0 to rakamArray.length-1){
      rakamArray(i) = i
      println(rakamArray(i))
    }

    for (i <- 0 until rakamArray.length) {
      rakamArray(i) = i
      println(rakamArray(i))
    }
    rakamArray(19)= 150000
    rakamArray.foreach(println)

    val insanlar = Array("ahmet","mehmet")
    var insanlar2 = insanlar ++ Array("Şükrü")
    insanlar2.foreach(println)

    val meyveler = ArrayBuffer[String]()
    meyveler.insert(0,"Elma")
    println("meyveler: "+meyveler)

    meyveler += "Portakal"
    println("meyveler: "+ meyveler)

    meyveler ++= Array("Muz","Armut")
    println("meyveler: "+ meyveler)

    meyveler.insert(1,"Nar","Üzüm","Karpuz","Kivi")
    println(meyveler)
    println(meyveler(5))

    println("yield ile liste biriktirme ve başka bir değişkene atama")

    val meyvelerBuyuk = for(meyve <- meyveler) yield meyve.toUpperCase()
    println("Meyveler büyük")
    meyvelerBuyuk.foreach(println)
  }
}
