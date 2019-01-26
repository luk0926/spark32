package cn.edu360.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/4.
  *
  * 自定义任务资源：
  *     --executor-cores : 每个executor上的cores
  *     --executor-memory : 每个executor上的内存
  *     --total-executor-cores : application上的最大的cores
  *
  *  RDD：
  *     弹性分布式数据集
  *     不可变、只读的、被分区的
  *     操作RDD就像操作本地集合一样，不需要关心底层调用原理
  *
  *     创建RDD的三种方式：
  *       集合并行化：sc.makeRDD()
  *       读取外部文件系统：sc.textFile()
  *       通过转换类的算子
  *
  *    RDD上的分区：
  *       通过rdd.partitions.size查看RDD的分区数量
  *
  *       1.读取HDFS上的文件：
  *       有几个block块，就有几个分区，如果只有一个block块，那么至少有2个分区
  *       2.集合并行化：application上有多少个cores就有多少个分区
  *
  *       RDD上的算子：分为两类：转化类算子transformation，行动类算子action
  */
object MapDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List[Int](1,2,3,4,5,6,7,8))

    //map  映射  有多少条数据就会调用几次
    val rdd1: RDD[Int] = rdd.map(_*10)
    //rdd1.foreach(println)

    //mapPartitions 对一个分区的数据进行迭代，每一个分区都是一个迭代器  有多少个 分区就会被调用几次
    val rdd2: RDD[Int] = rdd.mapPartitions(t=>t.map(_*10))

    //mapPartitionsWithIndex  带分区编号
    val f=(i:Int,it:Iterator[Int]) =>{
      it.map(t=>println(s"i:$i, v:$t"))
    }

    rdd.mapPartitionsWithIndex(f).foreach(println)

  }
}
