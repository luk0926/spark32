package cn.edu360.day05.Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/8.
  *
  * 自定义排序：直接根据元组来封装排序条件
  */
object SortDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val arr = Array("shouji 999.9 100", "shoulei 22.9 10", "shoubiao 1999.9 10000", "shoukao 22.9 11")

    val d: RDD[String] = sc.makeRDD(arr)

    //根据价格的降序排序，如果价格相同，根据库存的升序排序
    val map: RDD[(String, Double, Int)] = d.map(t => {
      val split: Array[String] = t.split(" ")
      val product: String = split(0)
      val price: Double = split(1).toDouble
      val amount: Int = split(2).toInt

      (product, price, amount)
    })

    val by: RDD[(String, Double, Int)] = map.sortBy(t => (-t._2, t._3))

    by.foreach(println)

    sc.stop()
  }
}
