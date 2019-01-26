package cn.edu360.Test.Order

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/10.
  */
object OrderTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\order\\ip.txt")

    //ip的规则
    val ipRulesRDD: RDD[(Long, Long, String)] = file.map(t => {
      val split: Array[String] = t.split("\\|")
      val startIp: Long = split(2).toLong
      val endIp: Long = split(3).toLong
      val province: String = split(6)

      (startIp, endIp, province)
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
    val ipRulesArr: Array[(Long, Long, String)] = ipRulesRDD.collect()

    val provinceAndPrice: RDD[(String, Int)] = order.map(t => {
      val province: String = binarySearch(t._1, ipRulesArr)
      (province, t._2)
    })

    //分组聚合
    val result: RDD[(String, Int)] = provinceAndPrice.reduceByKey(_+_)

    //将数据写入到mysql中

    result.foreach(t=>{
      var conn:Connection = null
      var pstm:PreparedStatement = null

      try{
        val url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8"

        conn = DriverManager.getConnection(url, "root", "1234")

        val pstm2 = conn.prepareStatement("create table IF NOT EXISTS order_1(province varchar(20) , price int)")

        pstm2.execute()

        val sql = "insert into order_1 values(?,?)"

        pstm = conn.prepareStatement(sql)

        pstm.setString(1, t._1)
        pstm.setInt(2, t._2)

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
  def binarySearch(ipLong: Long, ipRulesArr: Array[(Long, Long, String)]):String = {
    var start = 0
    var end = ipRulesArr.length - 1

    while (start <= end) {
      val middle = (start + end) / 2

      if (ipLong >= ipRulesArr(middle)._1 && ipLong <= ipRulesArr(middle)._2) {
        return ipRulesArr(middle)._3
      } else if (ipLong < ipRulesArr(middle)._1) {
        end = middle - 1
      } else {
        start = middle + 1
      }
    }
    "unKnown"
  }
}
