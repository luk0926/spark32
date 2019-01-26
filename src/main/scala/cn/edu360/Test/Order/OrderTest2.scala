package cn.edu360.Test.Order

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.immutable

/**
  * Created by dell on 2018/6/10.
  */
object OrderTest2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\order\\ip.txt")

    //ip的规则
    val ipRulesRDD: RDD[(Long, Long, String, String)] = file.map(t => {
      val split: Array[String] = t.split("\\|")
      val startIp: Long = split(2).toLong
      val endIp: Long = split(3).toLong
      val province: String = split(6)
      val city:String = split(7)

      (startIp, endIp, province, city)
    })

    //读取order
    val file1: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\order\\orders.log")

    val order: RDD[(Long, Int)] = file1.map(t => {
      val split: Array[String] = t.split(" ")

      val id: String = split(0)
      val ipLong: Long = ip2Long(split(1))
      val typ: String = split(2)
      val pro: String = split(3)
      val price: Int = split(4).toInt

      (ipLong, price)
    })

    //将ipRulersRDD转换成本地集合
    val ipRulesArr: Array[(Long, Long, String, String)] = ipRulesRDD.collect()

    val provinceAndCity: RDD[(String,String)] = order.map(t => {
      val (province, city): (String,String) = binarySearch(t._1, ipRulesArr)
      (province, city)
    })

    //分组聚合
    val group: RDD[(String, Iterable[(String, String)])] = provinceAndCity.groupBy(t=>t._1)

    val values: RDD[(String, List[(String, Int)])] = group.mapValues(t => {
      val by: Map[(String, String), List[(String, String)]] = t.toList.groupBy(tp => (tp._1, tp._2))
      val toList: List[(String, Int)] = by.map(t => {
        (t._1._2, t._2.length)
      }).toList

      toList.sortBy(-_._2).take(1)
    })

    //对数据进行整理
    val result: RDD[(String, String)] = values.flatMap(t => {
      val map: List[(String, String)] = t._2.map(tp => {
        (t._1, tp._1)
      })
      map
    })


    //将数据写入到mysql中

    result.foreach(t=>{
      var conn:Connection = null
      var pstm:PreparedStatement = null

      try{
        val url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8"

        conn = DriverManager.getConnection(url, "root", "1234")

        val pstm2 = conn.prepareStatement("create table IF NOT EXISTS order_2(province varchar(20) , city varchar(20))")

        pstm2.execute()

        val sql = "insert into order_2 values(?,?)"

        pstm = conn.prepareStatement(sql)

        pstm.setString(1, t._1)
        pstm.setString(2, t._2)

        pstm.execute()
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if(pstm != null){
          pstm.close()
        }

        if(conn != null){
          conn.close()
        }
      }
    })

    sc.stop()
  }

  //定义一个方法，把ip转换成十进制
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //定义一个二分查找方法
  def binarySearch(ipLong: Long, ipRulesArr: Array[(Long, Long, String,String)]):(String, String) = {
    var start = 0
    var end = ipRulesArr.length - 1

    while (start <= end) {
      val middle = (start + end) / 2

      if (ipLong >= ipRulesArr(middle)._1 && ipLong <= ipRulesArr(middle)._2) {
        return (ipRulesArr(middle)._3, ipRulesArr(middle)._4)
      } else if (ipLong < ipRulesArr(middle)._1) {
        end = middle - 1
      } else {
        start = middle + 1
      }
    }
    ("unknown", "unknown")
  }
}
