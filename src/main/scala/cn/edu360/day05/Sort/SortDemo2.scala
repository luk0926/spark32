package cn.edu360.day05.Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/8.
  *
  * 自定义排序：把数据封装成类或者样例类，然后再排序
  *
  * 注意：1.如果是类，必须实现Orderedd特质，实现序列化
  *      2.如果是样例类，实现Ordered特质，可以不用实现序列化特质
  *      3.排序之后的返回值类型是RDD[类]
  */
object SortDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val arr = Array("shouji 999.9 100", "shoulei 22.9 10", "shoubiao 1999.9 10000", "shoukao 22.9 11")

    val d: RDD[String] = sc.makeRDD(arr)

    //根据价格降序，如果价格相同，根据库存升序排序
    val splitData: RDD[Products] = d.map(t => {
      val split: Array[String] = t.split(" ")

      //利用样例类来封装数据
      new Products(split(0), split(1).toDouble, split(2).toInt)
    })

    val by: RDD[Products] = splitData.sortBy(t=> t)

    by.foreach(println)

    sc.stop()
  }
}

//定义一个样例类
case class Products(val name:String, val price:Double, val amount:Int) extends Ordered[Products]{
  override def compare(that: Products): Int = {
    if(this.price == that.price){
      this.amount - that.amount
    }else{
      if(this.price - that.price >0) -1 else 1
    }
  }

  override def toString: String = s"Products($name, $price, $amount)"
}
