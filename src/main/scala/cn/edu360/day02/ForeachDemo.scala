package cn.edu360.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by dell on 2018/6/4.
  */
object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //指定3个分区
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1,2,3,4,5,6,7,8), 3)

    //foreach：action类算子，对RDD中每个元素进行操作，有多少元素，就会被调用多少次
    //rdd.foreach(println)

    //foreachPartition:对一个分区进行操作，action类算子，有多少分区就会被调用多少次
    rdd.foreachPartition(t=>println(t.map(tp=>tp)))
  }
}
