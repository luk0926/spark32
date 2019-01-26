package cn.edu360.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/4.
  */
object AppUpdate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //2017-08-14,涛哥,共享女友,360应用,北京,v1.0
    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\appUpdate\\appupgrade.txt")

    val map: RDD[(String, String, String, String, Double)] = file.map(t => {
      val split: Array[String] = t.split(",")
      val date: String = split(0)
      val name: String = split(1)
      val app: String = split(2)
      val from: String = split(3)
      val version: Double = split(5).substring(1).toDouble

      (date, name, app, from, version)
    })


    //分组
    val by: RDD[((String, String, String, String), Iterable[(String, String, String, String, Double)])] = map.groupBy(t=>(t._1, t._2, t._3, t._4))

    //去除重复数据
    val values: RDD[((String, String, String, String), List[Double])] = by.mapValues(t => {
      val toList: List[Double] = t.map(tp => {
        tp._5
      }).toList
      toList.distinct
    })

    //过滤掉只有一条数据的信息
    val filter: RDD[((String, String, String, String), List[Double])] = values.filter(t=>t._2.size>1)

    val values1: RDD[((String, String, String, String), (String, String))] = filter.mapValues(t => {
      ("v" + t.min, "v" + t.max)
    })

    //需求1  最小版本和最大版本
    //values1.foreach(println)

    //需求2：多行数据
    val values2: RDD[((String, String, String, String), List[(Double, Double)])] = filter.mapValues(t => {
      val tail: List[Double] = t.tail
      val zip: List[(Double, Double)] = t.zip(tail)
      zip
    })

    val map2: RDD[(String, String, String, String, String, String)] = values2.flatMap(t => {
      val map1: List[(String, String, String, String, String, String)] = t._2.map(tp => {
        (t._1._1, t._1._2, t._1._3, t._1._4, "v" + tp._1, "v" + tp._2)
      })
      map1
    })

    map2.foreach(println)
  }
}
