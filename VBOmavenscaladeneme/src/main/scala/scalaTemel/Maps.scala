package scalaTemel

import scala.collection.mutable
import scala.collection.parallel.immutable
object Maps {
  def main(args: Array[String]): Unit = {
    val ulkeBaskent = Map("Japonya" -> "Tokyo",
      "Hindistan" -> "Delhi",
      "Güney Kore" -> "Seul"
    )
    println(s"ulkeBaskent: $ulkeBaskent")

    val ulkeBaskent2 = Map(("ABD", "Washington"), ("Fransa", "Paris"))
    println("Ülke iki " + ulkeBaskent2)

    val ulkeBaskentMut = mutable.Map("Japonya"->"Tokyo","Hindistan"-> "Delhi","Güney Kore"->"Seul")

    ulkeBaskentMut("Almanya") = "Berlin"

    ulkeBaskentMut += ("İspanya"->"Madrid")

    println(ulkeBaskentMut)

    val ogrenciler = mutable.Map(1503-> "Salih",1504->"Hasan",1505-> "Hüseyin")

    println(ogrenciler.get(1500)) // Eğer get kullanmazsak olmayanlar için hata verir

    println("keyler"+ ulkeBaskent2.keys)
    println("values"+ ulkeBaskent2.values)




  }

}
