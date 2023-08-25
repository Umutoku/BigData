package scalaTemel

object Functions {
  def main(args: Array[String]): Unit = {

    def topla(sayi1: Int, sayi2: Int): Int = {
      return sayi1 + sayi2
    }

    val toplam  = topla(2,2)
  println(toplam)

    def kendiniTanit(isim:String, yas:Int):Unit={
      println(s"Benim adım $isim, yaşım $yas")
    }

    kendiniTanit("Umut", 29)

    def sayilarTopla(args: Int*):Int={
      var toplam =0
      for(i<-args){
        toplam+=i
      }
      toplam
    }

    println(s"args ile sayıların toplamı: ${sayilarTopla(1,2,3,4,5)}")

    val ikiSayiTopla = (x:Int,y:Int)=>x+y

    println(s"İki sayının fonksiyon toplamı: ${ikiSayiTopla(4,5)}")

    def ikiSayiCarpimveToplam(x:Double,y:Double):(Double,Double)={
      var carpim:Double =0
      var toplam:Double =0
      carpim= x*y
      toplam = x+y

      (carpim,toplam)
    }

    println(s"İki sayının çarpımı ve toplamı: ${ikiSayiCarpimveToplam(2,5)}")

    def ikiSayiCarpimveToplam2(x:Double,y:Double):(Double, Double)={
      (x*y,x+y)
    }
    println(s"İki sayının çarpımı ve toplamı: ${ikiSayiCarpimveToplam2(2,5)} kısa yöntem")

    val (carpmaSonuc, toplamaSonuc) = ikiSayiCarpimveToplam2(25.0,30.0)
    println(s"Çarpma: $carpmaSonuc ve toplama: $toplamaSonuc" )

    val iliSayiCarpimveToplamDF = (x:Double,y:Double)=> {(x*y,x+y)}

    println(s"Çarpım ve toplam ${iliSayiCarpimveToplamDF(2,5)}")
  }

}

