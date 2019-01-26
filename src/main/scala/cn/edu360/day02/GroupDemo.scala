package cn.edu360.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/4.
  */
object GroupDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("aa", "bb", "cc"))
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3))

    val rdd2: RDD[(String, Int)] = rdd zip rdd1

    //groupBy, 返回值类型为 RDD((K, Iteratable(k,v)))
    val by: RDD[(String, Iterable[(String, Int)])] = rdd2.groupBy(_._1)
    //by.foreach(println)

    //groupByKey, 返回值类型 RDD((K, Iterable(V)))
    val key: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    //key.foreach(println)

    //reduceByKey,  返回值类型 RDD((K,V))
    val key1: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    key1.foreach(println)
  }
}
