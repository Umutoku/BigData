package scalaTemel

import scala.util.Random

object Odev {
  def main(args: Array[String]): Unit = {
    //Soru1

    val sayi1 = 3
    val sayi2 = 7
    println(s"$sayi1 sayısının $sayi2. kuvveti ${math.pow(sayi1,sayi2)}")

    //Soru 2
    var karekok = math.sqrt(1264)
    printf("%.2f",karekok)

      //Soru 3

    println(133%5)

    //soru 4
    val myList = List(9,5,15,9,63,7,88,25,15,5,79,15,121,7)

    val mySet = myList.toSet.toList.sorted // Önce sete sonra tekrar listeye

    println(mySet)

    var myTuple = ("Ali",5,(8,9),123,("Cemal,Fatma"),23)

    println(myTuple._5)

    //Soru 6
    val rassalSayiUretici = Random
    rassalSayiUretici.setSeed(42)

    println(rassalSayiUretici.nextFloat())

    // soru 7

    var karisik = "kjlasdjsadkllkjlaskjlaswvb"
    var oruntu = "kjl"
    var sayac = 0

    while(karisik.contains(oruntu)){
      karisik = karisik.replaceFirst(oruntu,"AAA")
      sayac+=1
    }
    println(karisik,sayac)








  }

}
