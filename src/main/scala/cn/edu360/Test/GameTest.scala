package cn.edu360.Test

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/6.
  */
object GameTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\GameTest\\user.log")

    //对数据进行切割
    val split: RDD[Array[String]] = file.map(t=>t.split("\\|"))
    //过滤掉缺失的的数据
    val filter: RDD[Array[String]] = split.filter(t=>t.length>8)
    //将数据组装成元组
    val map: RDD[(Int, String, String, String)] = filter.map(t => {
      val logType: Int = t(0).toInt
      val date: String = t(1).split(",")(0)
      val name: String = t(3)
      val actor: String = t(4)
      (logType, date, name, actor)
    })

    //根据日志类型和日期进行分组
    //val group: RDD[((Int, String), Iterable[(Int, String, String, String)])] = map.groupBy(t=>(t._1,t._2))

    //将Value转换成List类型，求出其长度
    //val values: RDD[((Int, String), Int)] = group.mapValues(t=>t.toList.length)

    //需求1：20160201当日新增玩家数
    //过滤出20160201当日新增用户
    //val filter1: RDD[((Int, String), Int)] = values.filter(t=>t._1._1==1 && t._1._2=="2016年2月1日")

    //filter1.foreach(println)
    //((1,2016年2月1日),188)
    //需求1：20160201新增玩家188


    //需求2：20160202玩家留存
    //根据名字和角色进行分组
    val group2: RDD[((String, String), Iterable[(Int, String, String, String)])] = map.groupBy(t=>(t._3,t._4))

    val values: RDD[((String, String), List[(Int, String)])] = group2.mapValues(t => {
      val toList: List[(Int, String)] = t.map(tp => {
        (tp._1, tp._2)
      }).toList
      toList
    })

    //过滤出201602留存的玩家
    val filter2: RDD[((String, String), List[(Int, String)])] = values.filter(t=>t._2.contains((1,"2016年2月1日")) && t._2.contains((2, "2016年2月2日")))

    val result2: Double = filter2.count().toDouble / 188
    println(result2)
    //需求2：20160202留存率0.1702127659574468
  }
}
