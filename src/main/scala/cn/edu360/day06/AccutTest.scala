package cn.edu360.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/9.
  */
object AccutTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val d: RDD[Int] = sc.makeRDD(List[Int](1,2,3,4,5))

    //1.6版本
    val acc: Accumulator[Int] = sc.accumulator(0, "acc")

    //2.0版本
    val acc2: LongAccumulator = sc.longAccumulator("acc2")

    d.foreach(t=>{
      //acc.add(t)

      acc2.add(t)
    })

    //println(acc.value)
    println(acc2.value)
  }
}
