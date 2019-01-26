package cn.edu360.day05

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/9.
  */
object IpTest2 {

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
  def binarySearch(ipLong: Long, ipRulesArr: Array[(Long, Long, String)]): String = {
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


  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("Usage:cn.edu360.day05.IpTest2<args(0)><args(1)>")
    }

    val conf: SparkConf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    //读取ipaccess.log的数据
    val file: RDD[String] = sc.textFile(args(0))

    val ipLong: RDD[Long] = file.map(t => {
      val split: Array[String] = t.split("\\|")
      val ipLong: Long = ip2Long(split(1))
      ipLong
    })


    //读取ip.txt的数据
    val file1: RDD[String] = sc.textFile(args(1))

    val ipRulesRDD: RDD[(Long, Long, String)] = file1.map(t => {
      val split: Array[String] = t.split("\\|")
      val start: Long = split(2).toLong
      val end: Long = split(3).toLong
      val province: String = split(6)

      (start, end, province)
    })

    //RDD不能嵌套使用，将ipRulesRDD转换成本地集合
    val ipRulesArr: Array[(Long, Long, String)] = ipRulesRDD.collect()

    val provinceAndOne: RDD[(String, Int)] = ipLong.map(t => {
      val province: String = binarySearch(t, ipRulesArr)
      (province, 1)
    })

    val result: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)

    //将数据结果写入到mysql中
    result.foreachPartition(t=>{
      var conn:Connection = null
      var pstm:PreparedStatement = null

      try {
        val url = "jdbc:mysql://192.168.12.52:3306/test?characterEncoding=utf-8"

        conn = DriverManager.getConnection(url, "root", "1234")

        val pstm2 = conn.prepareStatement("create table if not exists access_log2(province varchar(20), count int)")

        pstm2.execute()

        val sql = "insert into access_log2 values(?,?)"

        pstm = conn.prepareStatement(sql)

        t.foreach(tp=>{
          pstm.setString(1, tp._1)
          pstm.setInt(2, tp._2)

          //执行写入
          pstm.execute()
        })

        //释放连接
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
}
