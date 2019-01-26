package cn.edu360.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/10.
  */
object Accut2Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val d: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))

    val acc: LongAccumulator = sc.longAccumulator("acc")

    var sum = 0

    d.foreach(t=>{
      var t1: Long = System.currentTimeMillis()

      sum += t

      acc.add(t1)

      println(acc.value)
    })


  }
}
