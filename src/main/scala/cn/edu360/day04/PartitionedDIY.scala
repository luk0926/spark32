package cn.edu360.day04

import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by dell on 2018/6/7.
  */
object PartitionedDIY {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //http://bigdata.edu360.cn/laozhang
    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\spark-02\\作业\\teacher.log")

    val map: RDD[((String, String), Int)] = file.map(t => {
      val split: Array[String] = t.split("/")
      val url: String = split(2)
      val name: String = split(3)

      val split1: Array[String] = url.split("\\.")
      val sub: String = split1(0)
      ((sub, name), 1)
    })

    val subArray: Array[String] = map.map(_._1._1).distinct().collect()

    val result: RDD[((String, String), Int)] = map.reduceByKey(_+_)

    //调用partitionBy算子，传递一个分区器
    val partitionData: RDD[((String, String), Int)] = result.partitionBy(new MyPartitioner(subArray))

    //分区内排序
    val finalResult: RDD[((String, String), Int)] = partitionData.mapPartitions(it => {
      it.toList.sortBy(-_._2).take(2).iterator
    })

    finalResult.foreach(println)

    sc.stop()
  }
}

//自定义一个分区器，继承Partitioner的父类，重写抽象方法
class MyPartitioner(subArray:Array[String]) extends Partitioner{
  //学科从0开始加编号
  private val index: Array[(String, Int)] = subArray.zipWithIndex

  //把array转换成Map
  private val subsAndIndex: Map[String, Int] = index.toMap

  //分区的数量
  override def numPartitions: Int = subArray.size

  //传递一个key的值，返回对应的分区编号
  override def getPartition(key: Any): Int = {
    val trueKeys: (String, String) = key.asInstanceOf[(String, String)]

    val subName: String = trueKeys._1

    subsAndIndex(subName)
  }
}