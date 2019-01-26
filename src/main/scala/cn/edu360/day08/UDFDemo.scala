package cn.edu360.day08

import cn.edu360.day08.IpTest.ip2Long
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/12.
  */
object UDFDemo {

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
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    //导入spark的隐式转换
    import spark.implicits._

    //读取ip.txt的文件
    val ip: Dataset[String] = spark.read.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\ip\\ip.txt")

    //对数据进行切割
    val ipRules: Array[(Long, Long, String)] = ip.map(t => {
      val split: Array[String] = t.split("\\|")
      val startIp: Long = split(2).toLong
      val endIp: Long = split(3).toLong
      val province: String = split(6)

      (startIp, endIp, province)
    }).collect()




    //读取ipaccess.log
    val ipaccess: Dataset[String] = spark.read.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\ip\\ipaccess.log")

    //对数据进行切割处理
    val ipLong: Dataset[Long] = ipaccess.map(t => {
      val split: Array[String] = t.split("\\|")
      val ipLong: Long = ip2Long(split(1))
      ipLong
    })

    val ipLongDF: DataFrame = ipLong.toDF("ip")

    //注册成临时表
    ipLongDF.createTempView("ipLong")

    //自定义一个UDF函数并注册
   spark.udf.register("ip2province", (ip:Long)=>{
     val provincne: String = binarySearch(ipRules, ip)
     provincne
   })

    spark.sql("select ip2province(ip) pro,count(*) from ipLong group by pro")
    .show()

    spark.stop()
  }
}
