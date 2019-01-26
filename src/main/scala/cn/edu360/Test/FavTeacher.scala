package cn.edu360.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/4.
  */
object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //http://bigdata.edu360.cn/laozhang
    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\spark-02\\作业\\teacher.log")

    val map: RDD[(String, String, Int)] = file.map(t => {
      val split: Array[String] = t.split("/")
      val url: String = split(2)
      val name: String = split(3)

      val split1: Array[String] = url.split("\\.")
      val sub: String = split1(0)

      (sub, name, 1)
    })

    //分组
    val by: RDD[((String, String), Iterable[(String, String, Int)])] = map.groupBy(t=>(t._1, t._2))
    val values: RDD[((String, String), Int)] = by.mapValues(t => {
      val map1: Iterable[Int] = t.map(tp => {
        tp._3
      })
      map1.sum
    })

    //需求1，统计最受欢迎的老师
   // values.sortBy(-_._2).foreach(println)


    //需求2：统计学科的最受欢迎的老师
    val map1: RDD[(String, String, Int)] = values.map(t => {
      (t._1._1, t._1._2, t._2)
    })

    //取每个学科的 top1
    val by1: RDD[(String, Iterable[(String, String, Int)])] = map1.groupBy(t=>t._1)

    val values1: RDD[(String, List[(String, String, Int)])] = by1.mapValues(t => {
      val take: List[(String, String, Int)] = t.toList.sortBy(-_._3).take(1)
      take
    })

    //对数据进行整理
    val map3: RDD[(String, String, Int)] = values1.flatMap(t => {
      val map2: List[(String, String, Int)] = t._2.map(tp => {
        (t._1, tp._2, tp._3)
      })
      map2
    })

    //map3.foreach(println)

  }
}
