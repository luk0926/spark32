package cn.edu360.day05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/9.
  *
  * RDD的高级特性：
  *
  * 缓存持久化：
  *     cache()
  *     persist()
  *
  *     1.调用cache或者persist之后，必须出发action类算子，才回写入相应的介质中
  *     2.rdd之间依赖 关系不会改变
  *     3.如果内存足够，那么所有数据都会写入到介质中，如果内存不足，那么只会写入部分分区的数据，
  *       甚至一个分区的数据都没有cache
  *
  *     移除缓存：rdd.unpersist
  *
  *     缓存和持久化，将数据写入到内存和磁盘中，rdd之间的依赖关系不会改变，。如果缓存成功，去缓存或持久化
  *的介质中读取数据，如果失败，直接根据rdd之间的依赖关系读取数据
  *
  * checkpoint:
  *     1.需要在SparkContext对象上设置checkPoint的目录，必须是分布式的文件目录 sc.setCheckPointDir("hdfs:11cts01:9000/")
  *     2.当在某一个rdd上执行checkPoint的时候，并没有写入文件，当执行action的时候，才会触发任务
  *     3.checkpoint会产生两个job，一个正常运行任务，另一个执行写入文件到hdfs中
  *     4.checkpoint之后RDD之间的依赖关系全部删除，新的父依赖：checkPointRDD， 后面的rdd都
  *       以checkPointRDD作为起点
  *     5.checkPoint的目录，是可以多个application共享的
  *
  *
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

  //定义一个二分查找的方法
  def binarySearch(ipLong:Long, ipRulesArr: Array[(Long, Long, String)]):String = {
    var start = 0
    var end = ipRulesArr.length-1

    while(start <= end){
      val middle = (start + end) /2

      if(ipLong>= ipRulesArr(middle)._1 && ipLong <= ipRulesArr(middle)._2){
        return ipRulesArr(middle)._3
      }else if(ipLong < ipRulesArr(middle)._1){
        end = middle -1
      }else{
        start = middle + 1
      }
    }

    "unKnown"
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //读取ipaccess.log的数据
    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\ip\\ipaccess.log")

    val ipLong: RDD[Long] = file.map(t => {
      val split: Array[String] = t.split("\\|")
      val ipLong: Long = ip2Long(split(1))
      ipLong
    })


    //读取ip.txt的数据
    val file1: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\ip\\ip.txt")

    val ipRulesRDD: RDD[(Long, Long, String)] = file1.map(t => {
      val split: Array[String] = t.split("\\|")
      val start: Long = split(2).toLong
      val end: Long = split(3).toLong
      val province: String = split(6)

      (start, end, province)
    })

    //RDD不能嵌套使用，
    val ipRulesArr: Array[(Long, Long, String)] = ipRulesRDD.collect()

    val provinceAndOne: RDD[(String, Int)] = ipLong.map(t => {
      val province: String = binarySearch(t, ipRulesArr)

      (province, 1)
    })

    //分组聚合
    val result: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)

    //result.foreach(println)

    //将数据结果写入到mysql中
    result.foreachPartition(t=>{
      var conn:Connection = null
      var pstm:PreparedStatement = null

      try {
        val url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8"

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
