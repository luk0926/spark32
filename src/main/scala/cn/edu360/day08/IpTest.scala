package cn.edu360.day08

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/12.
  *
  * 求归属地案例
  */
object IpTest {

  //定义一个方法，把ip转换成十进制
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
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
    val ipRulesDS: Dataset[(Long, Long, String)] = ip.map(t => {
      val split: Array[String] = t.split("\\|")
      val startIp: Long = split(2).toLong
      val endIp: Long = split(3).toLong
      val province: String = split(6)

      (startIp, endIp, province)
    })

    //自定义schema信息
    val ipDF: DataFrame = ipRulesDS.toDF("start", "end", "province")

    //注册成临时表
    ipDF.createTempView("ipRules")


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

    //spark.sql("select province,count(*) from ipLong join ipRules on (ip >= start and ip <= end) group by province")
    //.show()

    spark.sql("select province,count(*) cnts from ipLong t1 join ipRules t2 on t1.ip between t2.start and " +
      "t2.end group by province order by cnts")
      .show()

    spark.stop()
  }
}
