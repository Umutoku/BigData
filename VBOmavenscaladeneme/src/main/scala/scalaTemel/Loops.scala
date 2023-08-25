package scalaTemel

object Loops {
  def main(args: Array[String]): Unit = {
    println("while")
    var i= 0
    do {
      println(i)
      i += 5
    }while (i < 10)

    forr

    println()

    println("for kelime örneği")
    val kelime = "Scala"
    for(i<-0 until kelime.length){
      println(kelime(i))
    }

    val listem = List(0,"Selam",1,"bysss",2,3,4,5)
    for(i<-listem){
      println(i)
    }

    val tekNumaralar = for{
      i<-1 to 30
      if(i%2==1)
    }yield i
    println(tekNumaralar)
    tekNumaralar.foreach(println)

    val meyveler = List("Elma","Muz","Nar")
    meyveler.foreach(x=>{println(x.toUpperCase())})
    }
def forr {
  println("for")
  for (i <- 0 to 10 by 2) { //hem 0 hem 10 dahil-- by ile artış miktarı
    println(i)
  }
}
}
