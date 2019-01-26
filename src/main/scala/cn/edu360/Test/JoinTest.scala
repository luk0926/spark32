package cn.edu360.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/6.
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\join\\a.txt")

    val rdd1: RDD[(String, (String, String))] = file.map(t => {
      val split: Array[String] = t.split(" ")
      val id: String = split(0)
      val age: String = split(1)
      val name: String = split(2)

      (id, (age, name))
    })

    val file1: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\join\\b.txt")

    val rdd2: RDD[(String, (String, String, String))] = file1.map(t => {
      val split: Array[String] = t.split(" ")
      val id: String = split(0)
      val year: String = split(1)
      val month: String = split(2)
      val movie: String = split(3)

      (id, (year, month, movie))
    })

    val join: RDD[(String, ((String, String), Option[(String, String, String)]))] = rdd1.leftOuterJoin(rdd2)

    val values: RDD[(String, (String, String, String, String, String))] = join.mapValues(t => {
      val tuple: (String, String, String) = if (t._2 == None) {
        (null, null, null)
      } else {
        t._2.get
      }

      (t._1._1, t._1._2, tuple._1, tuple._2, tuple._3)
    })

    //(u2,(15,xx,2017,12,m2))
    val map: RDD[((String, String, String), (String, String, String))] = values.map(t => {
      ((t._1, t._2._1, t._2._2), (t._2._3, t._2._4, t._2._5))
    })


    //分组  排序
    val key: RDD[((String, String, String), Iterable[(String, String, String)])] = map.groupByKey()

    val values1: RDD[((String, String, String), List[(String, String, String)])] = key.mapValues(t => {
      val sort: List[(String, String, String)] = t.toList.sortBy(_._1)
      sort
    })

    //对数据进行整理
    //((u3,18,aaa),List((2012,3,m5), (2014,2,m4), (2017,1,m3)))
    val map1: RDD[(String, String, String, String)] = values1.map(t => {
      (t._1._1, t._1._2, t._1._3, t._2.mkString(", "))
    })

    map1.foreach(println)
  }
}
