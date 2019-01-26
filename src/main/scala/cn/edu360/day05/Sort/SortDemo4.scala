package cn.edu360.day05.Sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by dell on 2018/6/8.
  *
  *  自定义排序：利用隐式转换，类不需要实现Ordered特质了
  */
object SortDemo4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val arr = Array("shouji 999.9 100", "shoulei 22.9 10", "shoubiao 1999.9 10000", "shoukao 22.9 11")

    val d: RDD[String] = sc.makeRDD(arr)

    val splitData: RDD[(String, Double, Int)] = d.map(t => {
      val split: Array[String] = t.split(" ")

      (split(0), split(1).toDouble, split(2).toInt)
    })

    //定义一个 隐式转换，从Products3 --> Ordered[Products3]
    //隐式方法  把Products3 转换成 Ordered[Products3]
    implicit def pro2OrderedPro(p:Products3):Ordered[Products3] = new Ordered[Products3]{
      override def compare(that: Products3): Int = {
        //要求返回值类型是int
        if(p.price == that.price){
          p.amount - that.amount
        }else{
          if(that.price -p.price > 0) 1 else -1
        }
      }
    }

    val by: RDD[(String, Double, Int)] = splitData.sortBy(t => Products3(t._1, t._2, t._3))

    by.foreach(println)

    sc.stop()
  }
}

case class Products3(val name:String, val price:Double, val amount:Int){
  override def toString: String = s"Products3($name, $price, $amount)"
}