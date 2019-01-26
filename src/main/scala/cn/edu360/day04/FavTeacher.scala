package cn.edu360.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/7.
  */
object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\favTeacher\\teacher.log")

    val map: RDD[((String, String), Int)] = file.map(t => {
      val split: Array[String] = t.split("/")
      val sub: String = split(2).split("\\.")(0)
      val name: String = split(3)

      ((sub, name),1)
    })

    val key: RDD[((String, String), Int)] = map.reduceByKey(_+_)

    //全局topN
    //key.sortBy(-_._2).foreach(println)

    val by: RDD[(String, Iterable[((String, String), Int)])] = key.groupBy(t=>t._1._1)

    val values: RDD[(String, List[((String, String), Int)])] = by.mapValues(t => {
      val take: List[((String, String), Int)] = t.toList.sortBy(-_._2).take(2)
      take
    })

    values.foreach(println)
  }
}