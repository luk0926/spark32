package cn.edu360.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/3.
  *
  * Spark:
  *       spark是使用scala语言编写的，基于内存的、分布式大数据处理框架（大数据计算引擎）
  *
  * Spark的特点：
  *       1.速度快
  *       2.易用性
  *       3.通用性
  *       4.兼容性
  *
  * Spark的四种部署模式：
  *       1.local 本地模式
  *       2.standalone模式
  *       3.yarn集群模式
  *       4.mesos集群模式
  *
  * 两个提交命令：
  *       spark-shell:命令行客户端
  *       spark-submit --master 指定集群模式 --class 指定运行的main方法 jar包 main方法的参数列表
  *
  * Spark中的各个角色：
  *     两个常驻角色：Master, worker
  *
  *     spark-submit:在客户端出现
  *     executor:在worker节点
  *
  * Spark运行的资源分配：
  *     spark运行时，默认情况下，wordker占用所用的cores,占用的内存：所有的内存-1G
  *
  *     默认情况下，每一个wordker节点上，启用一个executor
  *     每一个executor占用所有的cores，占用1G内存
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("Usage:cn.edu360.day01.WordCount<input><output>")
      sys.exit(1)
    }
    val Array(input, output) = args

    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile(input)
    val map: RDD[String] = file.flatMap(_.split(" "))
    val map1: RDD[(String, Int)] = map.map((_, 1))
    val key: RDD[(String, Int)] = map1.reduceByKey(_+_)

    key.saveAsTextFile(output)

    sc.stop()
  }
}
