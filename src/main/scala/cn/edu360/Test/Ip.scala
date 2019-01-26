package cn.edu360.Test

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/8.
  */
object Ip {

  //定义一个方法，把ip转换成十进制
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //定义一个二分查找的方法
  def binarySearch(ipRules: Array[(Long, Long, String)], ipLong: Long): String = {
    //定义两个索引
    var startIndex = 0
    var endIndex = ipRules.length - 1

    //循环判断
    while (startIndex <= endIndex) {
      var middleIndex = (startIndex + endIndex) / 2

      //接收元组的返回值
      val (start, end, province) = ipRules(middleIndex)

      //进行比较
      if (ipLong >= start && ipLong <= end) {
        return province
      } else if (ipLong < start) {
        endIndex = middleIndex - 1
      } else {
        startIndex = middleIndex + 1
      }
    }

    "unknown"
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //读取ipaccess.log的数据
    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\ip\\ipaccess.log")

    //获取到每条信息ip的长整型数据
    val ipLong: RDD[Long] = file.map(t => {
      val split: Array[String] = t.split("\\|")
      val ipStr: String = split(1)
      val ipLong: Long = ip2Long(ipStr)
      ipLong
    })

    //读取ip.txt的数据
    val file1: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\ip\\ip.txt")

    val ipRulesRdd: RDD[(Long, Long, String)] = file1.map(t => {
      val split: Array[String] = t.split("\\|")
      val startIpLong: Long = split(2).toLong
      val endIpLong: Long = split(3).toLong
      val province: String = split(6)
      val city: String = split(7)

      (startIpLong, endIpLong, province)
    })


    //把provinceAndIp收集到 Driver端
    val ipRules: Array[(Long, Long, String)] = ipRulesRdd.collect()

    val provinceWithOne: RDD[(String, Int)] = ipLong.map(t => {
      val province: String = binarySearch(ipRules, t)

      (province, 1)
    })

    //分组聚合
    val result: RDD[(String, Int)] = provinceWithOne.reduceByKey(_ + _)

    //result.foreach(println)


    result.foreachPartition(t => {
      var conn: Connection = null
      var pstm: PreparedStatement = null

      try {
        val url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8"

        conn = DriverManager.getConnection(url, "root", "1234")

        val pstm2 = conn.prepareStatement("create table IF NOT EXISTS access_log(province varchar(20) , counts int)")

        pstm2.execute()

        val sql = "insert into access_log values(?,?)"

        pstm = conn.prepareStatement(sql)

        t.foreach(tp => {
          //赋值
          pstm.setString(1, tp._1)
          pstm.setInt(2, tp._2)

          //执行写入
          pstm.execute()
        })

        //释放连接
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (pstm != null) {
          pstm.close()
        }

        if (conn != null) {
          conn.close()
        }
      }
    })

    sc.stop()
  }
}
