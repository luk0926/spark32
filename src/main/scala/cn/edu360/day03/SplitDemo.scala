package cn.edu360.day03

/**
  * Created by dell on 2018/6/6.
  */
object SplitDemo {
  def main(args: Array[String]): Unit = {
    val str:String = "a,b,,,,,"

    //参数，切分后长度为2
    println(str.split(",").length)
    //指定第二个参数为-1，切分后长度为7
    println(str.split(",", -1).length)
  }
}
