package cn.edu360.day01

import com.google.protobuf.ByteString.Output
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/3.
  */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage:cn.edu360.day01.WordCount2<host><port>")
      sys.exit(1)
    }

    val Array(input, output) = args

    val conf: SparkConf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    //读取文件
    val file: RDD[String] = sc.textFile(input)
    //切割并压平
    val map: RDD[String] = file.flatMap(_.split(" "))
    //和1组装
    val map1: RDD[(String, Int)] = map.map((_, 1))
    //分组聚合
    val key: RDD[(String, Int)] = map1.reduceByKey(_ + _)
    //保存文件
    key.saveAsTextFile(output)

    sc.stop()
  }
}
