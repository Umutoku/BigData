package scalaTemel

object PrintFormatting {
  def main(args: Array[String]): Unit = {
    val isim = "Emre"
    val yas = 35
    val boy = 1.87

    println(s"Merhaba $isim")

    println(f"Benim adım $isim ev ben $boy%.2f boyundayım")

    printf("%02d",5)

    printf("%.3f",5.1)
  }

}
