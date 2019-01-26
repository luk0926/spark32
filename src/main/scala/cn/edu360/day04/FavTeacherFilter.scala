package cn.edu360.day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by dell on 2018/6/7.
  *
  * 通过过滤的方式得到分组topN
  */
object FavTeacherFilter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //http://bigdata.edu360.cn/laozhang
    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\spark-02\\作业\\teacher.log")

    val map: RDD[((String, String), Int)] = file.map(t => {
      val split: Array[String] = t.split("/")
      val url: String = split(2)
      val name: String = split(3)

      val split1: Array[String] = url.split("\\.")
      val sub: String = split1(0)
      ((sub, name), 1)
    })

    //过滤出bigData的数据
    val bigdata: RDD[((String, String), Int)] = map.filter(t=>t._1._1 == "bigdata")

    val key: RDD[((String, String), Int)] = bigdata.reduceByKey(_+_)

    val by: RDD[((String, String), Int)] = key.sortBy(-_._2)

    by.take(2).foreach(println)

    //过滤出php的数据
    val php: RDD[((String, String), Int)] = map.filter(t=>t._1._1 == "php")

    val key1: RDD[((String, String), Int)] = php.reduceByKey(_+_)

    val by1: RDD[((String, String), Int)] = key1.sortBy(-_._2)

    by1.take(2).foreach(println)

    //过滤出javase的数据
    val javaee: RDD[((String, String), Int)] = map.filter(t=>t._1._1 == "javaee")

    val key2: RDD[((String, String), Int)] = javaee.reduceByKey(_+_)

    val by2: RDD[((String, String), Int)] = key2.sortBy(-_._2)

    by2.take(2).foreach(println)
  }
}
