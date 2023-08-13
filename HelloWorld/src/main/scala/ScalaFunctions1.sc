import scala.language.postfixOps

def JustPrint() = {
  print("Merhaba fonksiyon")
}
JustPrint()

def JustPrint2():Unit = {
  print("Merhaba fonksiyon")
}
JustPrint2()

def addTo(m: Int):Int = {
  m+2
}

print(addTo(2))

def addTo2() : Int = {
  2+1
}

addTo2()
addTo2

addTo2().toString
addTo2() toString // sugar

def add(m: Int = 2,n: Int): Unit = {
  m+n
}
add(5,6)
add(n = 2)

def `combine umut oku`(str: String = "hello", n: Int = 1): String = {
  s"$str and $n"
}
//print(combine())

`combine umut oku`()