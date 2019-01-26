package cn.edu360.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/4.
  */
object AvgTemTest {
  def main(args: Array[String]): Unit = {
    val d1 = Array(("bj",28.1), ("sh",28.7), ("gz",32.0), ("sz", 33.1))
    val d2 = Array(("bj",27.3), ("sh",30.1), ("gz",33.3))
    val d3 = Array(("bj",28.2), ("sh",29.1), ("gz",32.0), ("sz", 30.5))

    val d: Array[(String, Double)] = d1 union d2 union d3

    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //将数据转换成RDD
    val rdd: RDD[(String, Double)] = sc.makeRDD(d)
    //分组
    val groupRDD: RDD[(String, Iterable[(String, Double)])] = rdd.groupBy(_._1)
    //聚合
    val result: RDD[(String, Double)] = groupRDD.mapValues(t => {
      val map: Iterable[Double] = t.map(tp => {
        tp._2
      })
      map.sum / map.size
    })

    result.foreach(println)
  }
}
