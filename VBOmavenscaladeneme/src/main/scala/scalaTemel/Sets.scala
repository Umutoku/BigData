package scalaTemel

object Sets {
  def main(args: Array[String]): Unit = {
    // Boş immutable bir Set oluşturmak
    val myImmutSet = Set(1,1,1,2,3,4,5,5,4,6,4,3) //Setlerde aynı eleman bir tane olabilir distinct

    val MyImmutSet1 = Set("Merhaba",1,2,3,4,"Selam")

    println(MyImmutSet1)

    //Setlerde index yoktur
    // eleman varlığı çalışmaz, var mı yok mu diye çalışır

    println("myImmutSet1 içinde var mı: " + MyImmutSet1(4))

    val myMuteSet = scala.collection.mutable.Set(1,2,3,4,5)

    myMuteSet += 4

    myMuteSet.add(99)

    val myList = List(9,2,3,4,5,6)
    val setFromList = myList.toSet
    println(setFromList)

    //iki kümenin kesişim noktası
    println("Intersect" + setFromList.intersect(myMuteSet))

    //ilk listede olup ikinci listede olmayan
    println("Different" + setFromList.diff(myMuteSet))


  }

}
