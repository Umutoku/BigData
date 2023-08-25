package scalaTemel

object Strings {
  def main(args: Array[String]): Unit = {
    var cumle = "Ali ata bak"

    println("4. indis: "+ cumle(4))

    for(harf<-cumle) println(harf)
    println()
    println(cumle.substring(3,5))

    println(cumle.contains("i"))

    println(cumle.concat(" Emel eve gel."))

    println(cumle.endsWith("."))

    println(cumle.hashCode)

    println(cumle.replace("Ali","Umut"))

  }

}
