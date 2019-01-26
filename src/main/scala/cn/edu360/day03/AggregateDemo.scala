package cn.edu360.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/6.
  */
object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)

    //aggregate是一个action类的算子
    //第一个括号内传递一个初始值；第二个括号内传递两个函数，第一个为局部聚合，第二个为全局聚合
    ///初始值既参与局部聚合，有多少个分区就参与多少次局部聚合，也参与全局聚合
    val r1: Int = rdd.aggregate(0)(_+_, _+_)
    val r2: Int = rdd.aggregate(10)(_+_, _+_)
    //println(r2)

    //aggregateByKey:是一个transformation类的算子
    //第一个括号内传递一个初始值，第二个括号内传递两个函数：第一个是局部聚合，第二个为全局聚合
    //初始值只参与局部聚合，不参与全局聚合
    val d: RDD[(String, Int)] = sc.makeRDD(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    val d1: RDD[(String, Int)] = d.aggregateByKey(0)(_+_,_+_)
    val d2: RDD[(String, Int)] = d.aggregateByKey(10)(_+_,_+_)
    //d1.foreach(println)
    d2.foreach(println)
  }
}
