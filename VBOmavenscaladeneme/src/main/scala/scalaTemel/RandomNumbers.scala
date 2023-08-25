package scalaTemel

import scala.util.Random

object RandomNumbers {
  def main(args: Array[String]): Unit = {
    val r = Random

    r.setSeed(seed=1000L)

    println(r.nextInt)

    //Rastgele int sınır
    println(r.nextInt(100))

    // sıfır ve 1 arasında
    println(r.nextFloat())

    println(r.nextPrintableChar())

    println(r.nextString(10))
  }

}
