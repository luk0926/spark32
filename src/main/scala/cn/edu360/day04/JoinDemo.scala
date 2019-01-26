package cn.edu360.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/8.
  */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //读取文件
    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\join\\a.txt")
    val file1: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\join\\b.txt")

    //对a.txt进行分割
    val map: RDD[(String, String)] = file.map(t => {
      val split: Array[String] = t.split(" ", 2)
      val id: String = split(0)
      val ageAndName: String = split(1)

      (id, ageAndName)
    })

    //对b.txt进行分割
    val map1: RDD[(String, String)] = file1.map(t => {
      val split: Array[String] = t.split(" ", 2)
      val id: String = split(0)
      val yearAndmovie: String = split(1)

      (id, yearAndmovie)
    })


    val cogroup: RDD[(String, (Iterable[String], Iterable[String]))] = map.cogroup(map1)

    val values: RDD[(String, (Iterable[String], String))] = cogroup.mapValues(t => {
      val s: String = if (t._2.isEmpty) {
        "null null null"
      } else {
        val by: List[String] = t._2.toList.sortBy(_.split(" ")(0).toInt)
        by.mkString(" ")
      }

      (t._1, s)
    })

    values.foreach(println)

  }
}
