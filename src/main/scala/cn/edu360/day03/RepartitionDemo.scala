package cn.edu360.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/5.
  *
  * RDD五大特性：
  *   分区器、分区列表
  *   computer函数
  *   依赖关系
  *   优先位置
  */
object RepartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val d: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8),4)

    d.repartition(4)
    d.coalesce(4, true)
  }
}
