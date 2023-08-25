package scalaTemel

object Lists {
  def main(args: Array[String]): Unit = {
    //Bağlı liste (Linked list) ancak immutable

    //Sadece tam sayılardan oluşan bir liste
    val ciftNumaralar = List(2,4,6,8,10)
    println(ciftNumaralar)

    println("Uzunluk" + ciftNumaralar.size)

    println("Büyük: "+ ciftNumaralar.max + "Kücük" + ciftNumaralar.min)

    println("Kesit" + ciftNumaralar.slice(1,4))

    println("Eleman düşürme" + ciftNumaralar.drop(2)) // ilk indisten başlar

    println("Eleman düşürme" + ciftNumaralar.takeRight(2)) // son indisten başlar

    println(ciftNumaralar.map(x=>x*x)) //map ile listelerde işlem yapılır

    val harfler = List("a","b","c","d","e") // iki listeyi birleştirir. aynı uzunlukta olmalı
    println(harfler.zip(ciftNumaralar))

    val zipList = harfler.zip(ciftNumaralar)


  }

}
