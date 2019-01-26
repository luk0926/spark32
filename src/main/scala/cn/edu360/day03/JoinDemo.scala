package cn.edu360.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/5.
  *
  * RDD五大特性：
  *   分区列表
  *   分区器
  *   computer函数
  *   依赖关系
  *   优先存储位置
  */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val d: RDD[(String, Int)] = sc.makeRDD(List(("aa", 1), ("bb", 2), ("cc", 3), ("ee", 5)))
    val d1: RDD[(String, Double)] = sc.makeRDD(List(("aa", 9.9), ("bb", 19.8), ("cc", 2.5), ("dd", 6.6)))

    //内关联join:返回值类型 RDD(K, (V, W))
    val join: RDD[(String, (Int, Double))] = d join d1
    //join.foreach(println)

    //左外关联leftOuterJoin:返回值类型：RDD(K, (V, Option(W)))
    val leftOutJoin: RDD[(String, (Int, Option[Double]))] = d.leftOuterJoin(d1)
    //leftOutJoin.foreach(println)

    //右外关联rightOuterJoin:返回值类型：RDD(K, (Option(V), W))
    val rightOutJoin: RDD[(String, (Option[Int], Double))] = d.rightOuterJoin(d1)
    //rightOutJoin.foreach(println)

    //全外关联cogroup:返回值类型：RDD(K, (Iterable(v), Iterable(w)))
    val cogroup: RDD[(String, (Iterable[Int], Iterable[Double]))] = d.cogroup(d1)
    //cogroup.foreach(println)

    sc.stop()
  }
}
