package cn.edu360.Test.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/8.
  */
object UrlTest {
  import java.security.MessageDigest

  def md5Encoding(password: String):String = { // 得到一个信息摘要器
    val digest = MessageDigest.getInstance("md5")
    val result = digest.digest(password.getBytes)
    val buffer = new StringBuffer
    // 把没一个byte 做一个与运算 0xff;
    for (b <- result) { // 与运算
      val number = b & 0xff
      val str = Integer.toHexString(number)
      if (str.length == 1) buffer.append("0")
      buffer.append(str)
    }
    // 标准的md5加密后的结果
    buffer.toString
  }


  // 把url转换为 md5加密的
  def url2MD5(url: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    md5.update(url.getBytes())
    //加密后的字符串
    val digest = md5.digest()
    // 把字节数组转换为16进制
    toHexString(digest)
  }

  //转16进制
  def toHexString(b: Array[Byte]) = {
    val hexChar = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')
    val sb = new StringBuilder(b.length * 2)
    var i = 0
    while (i < b.length) {
      {
        sb.append(hexChar((b(i) & 0xf0) >>> 4))
        sb.append(hexChar(b(i) & 0x0f))
      }
      {
        i += 1
        i - 1
      }
    }
    sb.toString
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\url\\40690.txt")

    val map: RDD[(String, String)] = file.map(t => {
      val split: Array[String] = t.split("/")

      val split1: Array[String] = split(2).split("\t")
      val url: String = split1(0)
      val ty: String = split1(2)
      (url, ty)
    })

    val collect: Array[(String, String)] = map.collect()

    val map1: Array[(String, String)] = collect.map(t => {
      val d: String = url2MD5(t._1).substring(0, 14)
      (d, t._2)
    })

    //(60a12a6916c303,11)   map1

    val file1: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\url\\url.db1000")

    val map2: RDD[String] = file1.map(t => {
      val urldb: String = t.substring(0, 14)
      urldb
    })

    val collect1: Array[String] = map2.collect()

    // 00000471b4e012   collect1


    for(i <- collect1){
      for(j <- map1){
        if(i == j._1){
          println(i+j._2)
        }
      }
    }
  }
}
