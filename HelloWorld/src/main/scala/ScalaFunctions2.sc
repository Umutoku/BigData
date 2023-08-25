def anonim =(x: Int) => {
  x+2
}
anonim(2)
anonim.apply(2)

val anotherAnonim = () => "Merhaba canim"
anotherAnonim // bu şekilde fonksiyonun ne olduğunu döner
anotherAnonim.apply()
anotherAnonim()

def add3To (m: Int,n : Int):Int = {
  m+3
}

add3To(3,2)

val add3Func = add3To _

val result = add3Func.apply(2,3)

def multiplyBy(m: Int): Unit = {
  m+2
}
multiplyBy(3)

def multiply(m: Int,n: Int): Int = {
  m*n
}

val firstResult = multiply(2,_:Int)
firstResult(5)

def showAll(args:(Int,Int)*): Unit = {
  args.foreach(print(_))
}

def total=(str:String,n:Int)=> {
  n + str.length
}
total("ada",3)

def strIncome(str:String):(Int=>Int)= {
  total(str, _: Int)
}

val result = strIncome("Hello World")(.o92)