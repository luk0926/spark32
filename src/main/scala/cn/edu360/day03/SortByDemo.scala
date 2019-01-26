package cn.edu360.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/6.
  */
object SortByDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val d1 = Array(("bj", 28.1), ("sh", 28.7), ("gz", 32.0), ("sz", 33.1))

    val tmp: Array[Double] = d1.map(_._2)

    val d: RDD[Double] = sc.makeRDD(tmp)

    //sortBy默认是升序
    val by: RDD[Double] = d.sortBy(t=>t)
    //by.foreach(println)

    val d2: RDD[(String, Double)] = sc.makeRDD(d1)

    //sortByKey默认是升序的，根据字符串的字典顺序进行排序
    val key: RDD[(String, Double)] = d2.sortByKey()
    //key.foreach(println)

    ///通过指定sortByKey的参数为false来实现降序排列
    val key1: RDD[(String, Double)] = d2.sortByKey(false)
    key1.foreach(println)
  }
}
