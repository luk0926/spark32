package cn.edu360.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/8/12.
  */
object AvgTemTest2 {
  def main(args: Array[String]): Unit = {
    val d1 = Array(("bj",28.1), ("sh",28.7), ("gz",32.0), ("sz", 33.1))
    val d2 = Array(("bj",27.3), ("sh",30.1), ("gz",33.3))
    val d3 = Array(("bj",28.2), ("sh",29.1), ("gz",32.0), ("sz", 30.5))

    val d: Array[(String, Double)] = d1 union d2 union d3

    val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Double)] = sc.makeRDD(d)

    val key: RDD[(String, Iterable[Double])] = rdd.groupByKey()

    val re: RDD[(String, Double)] = key.map(t => {
      val sum: Double = t._2.sum
      val avg: Double = sum / t._2.size
      (t._1, avg)
    })

    re.foreach(println)

    sc.stop()
  }
}
