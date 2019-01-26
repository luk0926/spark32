package cn.edu360.day05.Sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by dell on 2018/6/8.
  *
  * 自定义排序：把数据用元组封装，在排序的时候，使用类或者样例类来指定排序规则
  *
  * 注意：1.如果是类，必须实现ordered特质，实现序列化特质，
  *       2.如果是样例类，可以不用实现序列化特质
  *       3.排序之类的返回值类型，是原来的类型
  */
object SortDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val arr = Array("shouji 999.9 100", "shoulei 22.9 10", "shoubiao 1999.9 10000", "shoukao 22.9 11")

    val d: RDD[String] = sc.makeRDD(arr)

    //用元组来封装数据
    val splitData: RDD[(String, Double, Int)] = d.map(t => {
      val split: Array[String] = t.split(" ")

      (split(0), split(1).toDouble, split(2).toInt)
    })

    val by: RDD[(String, Double, Int)] = splitData.sortBy(t=> new Products2(t._1, t._2, t._3))

    by.foreach(println)

    sc.stop()
  }
}

//使用样例类来封装数据
case class Products2(val name:String, val price:Double, val amount:Int) extends Ordered[Products2] {
  override def toString: String = s"Products2($name, $price, $amount)"

  override def compare(that: Products2): Int = {
    if(this.price == that.price){
      this.amount - that.amount
    }else{
      if(this.price - that.price >0) -1 else 1
    }

  }
}