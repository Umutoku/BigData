package scalaTemel


object Classes {
  def main(args: Array[String]): Unit = {

    val selamSabah = new Selam("Merhaba ","!")
    selamSabah.sabah("Umut bey")

    val selamSabah2 = new Selam("Hoşgeldiniz","Yine bekleriz")
    selamSabah2.sabah(" Ayşe teyze ")

    case class Point(x:Int,y:Int)

    val point1 = Point(1,2)
    val point2 = Point(2,3)

    if(point1==point2) println("Aynı") else println("Farklı")

    case class Ogrenci(isim:String, notu:Float)
    val ahmet = Ogrenci("Ahmet",45.77F)

    if(ahmet.notu<50) println("Kaldı") else println("Geçti")

    case class Calisan(isim:String="İsim yok",unvan:String="Unvan yok",maas:Int=30000)
    val sevda = new Calisan("Sevda","Analist",50000)

    println(s"${sevda.isim}, ${sevda.unvan} pozisyonunda aylık ${sevda.maas} ile çalışıyor.")

  }

}
