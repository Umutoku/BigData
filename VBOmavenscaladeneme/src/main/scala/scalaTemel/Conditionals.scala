package scalaTemel

object Conditionals {
  def main(args: Array[String]): Unit = {
    var yas = 18
    val oyVerebilirMi = if(yas >=18) "oy verebilir" else "oy veremez"
    println("Oy verebilir mi: "+ oyVerebilirMi)

    println()
    var enYuksekhizLimiti = 120.0
    var enDusukhizLimiti = 40.0

    var surucuHizi = 150
    var surucu = "Umut"

    if((surucuHizi<enYuksekhizLimiti)&&(surucuHizi>enDusukhizLimiti)){
      println(s"Sayın $surucu, yasal sınırlar içerisinde seyahat ediyorsunuz. Hızınız: $surucuHizi")
    } else {
      println(s"Sayın $surucu, yasal sınırlar içerisinde seyahat etmiyorsunuz. Hızınız: $surucuHizi")
    }
  }

}
