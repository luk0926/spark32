package cn.edu360.day06

import java.security.MessageDigest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/11.
  */
object UrlTest {

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

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\url\\url.db1000")

    val md5AndNew: RDD[(String, String)] = file.map(t => {
      val md5URL: String = t.substring(0, 14)
      val newType: String = t.substring(14)

      (md5URL, newType)
    })

    val file1: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\url\\40690.txt")

    val md5AndOld: RDD[(String, String)] = file1.map(t => {
      val split: Array[String] = t.split("/")
      val str: String = split(2)

      val split1: Array[String] = str.split("\t#\t")
      val md5Url: String = url2MD5(split1(0)).substring(0,14)
      val oldType: String = split1(1)
      (md5Url, oldType)
    })

    val join: RDD[(String, (String, Option[String]))] = md5AndOld.leftOuterJoin(md5AndNew)

    val filter: RDD[(String, (String, Option[String]))] = join.filter(t=>t._2._2 != None)

    val result: RDD[(String, String)] = filter.mapValues(t => {
      val newType: String = t._2.get
      newType
    })

    result.foreach(println)

    sc.stop()
  }
}
