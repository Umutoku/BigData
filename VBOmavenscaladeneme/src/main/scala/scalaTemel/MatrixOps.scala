package scalaTemel

import breeze.linalg.sum
import org.apache.spark.ml.linalg.{Matrices, Vectors}

object MatrixOps {
  def main(args: Array[String]): Unit = {

    val zeroMatrix = Matrices.zeros(5,5)
    val oneMatrix = Matrices.ones(5,5)
    //println(s"$zeroMatrix \nve \n$oneMatrix")

    val arrayMatrix = Matrices.dense(3,3,Array(1,2,3,4,5,6,7,8,9)).toDense
    val arrayMatrix2 = Matrices.dense(3,3,Array(3,5,7,9,11,13,15,17,19)).toDense

    val arrayMatrix3 = Matrices.dense(3,4,Array(1,1,1,1,2,2,2,0,3,5,4,7)).toDense
    val arrayMatrix4 = Matrices.dense(3,4,Array(1,0,1,1,2,8,2,0,0,5,2,7)).toDense

    println("Matrisin devriği")
    println(arrayMatrix3+ " \n" + arrayMatrix3.transpose)

    //Sıfır olmayan kaç değer var
    println("Sıfır olmayan değer sayısı: "+ arrayMatrix4.numNonzeros)

    //Satır ve sütün sayısını söyle
    println("Satır ve sütün sayısı: "+ arrayMatrix4.numRows,arrayMatrix4.numCols)

    //Matrisin elemanları toplamı
    println("Matrisin elemanlarının toplamı: "+ sum(arrayMatrix.values))
    println("Matrisin elemanlarının toplamı: "+ sum(arrayMatrix.toArray))

    val fromDiagonalMatrix = Matrices.diag(Vectors.dense(Array(1.0,5.0,9.0,11.0,13.0))).toDense
    println("Ana köşegenden matris elde etme: \n"+ fromDiagonalMatrix)

    val eyeMatrix = Matrices.eye(5).toDense
    println("Birim matris: \n"+ eyeMatrix)

    val multipliedMatrix = arrayMatrix.multiply(arrayMatrix2)
    println("Matris çarpımı: \n" + multipliedMatrix)


  }

}
