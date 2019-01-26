package cn.edu360.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/8/11.
  */
object WordCount3 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("Usage:cn.edu360.day01.WordCount3<input><output>")

      sys.exit(1)
    }

    val Array(input, output) = args

    val conf: SparkConf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    //读取文件
    val file: RDD[String] = sc.textFile(input)

    val split: RDD[String] = file.flatMap(_.split(" "))
    val map: RDD[(String, Int)] = split.map((_, 1))
    val re: RDD[(String, Int)] = map.reduceByKey(_ + _)

    re.saveAsTextFile(output)

    sc.stop()
  }
}
