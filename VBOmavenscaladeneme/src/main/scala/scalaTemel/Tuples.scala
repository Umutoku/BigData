package scalaTemel

object Tuples {
  def main(args: Array[String]): Unit = {

    var tupleAhmet = (1001,"Ahmet Yaman",86.77)


    println(tupleAhmet._2) //scala tupleda ._ ile elemana erişilir

    printf("%s'ın not ortalaması: %.2f\n",tupleAhmet._2,tupleAhmet._3)

    val(no,isim,notu) = tupleAhmet
    println(no,isim,notu)


  }
}
