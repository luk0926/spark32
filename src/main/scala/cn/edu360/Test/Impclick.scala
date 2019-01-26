package cn.edu360.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/4.
  */
object Impclick {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\spark-02\\作业\\展示量\\impclick.txt")
    //1010,华语剧场|剧情|当代|类型,1,0
    val map1: RDD[(String, String, Int, Int)] = file.flatMap(t => {
      val split: Array[String] = t.split(",")
      val id: String = split(0)
      val show: Int = split(2).toInt
      val impclick: Int = split(3).toInt

      val split1: Array[String] = split(1).split("\\|")
      val map: Array[(String, String, Int, Int)] = split1.map(t => {
        (id, t, show, impclick)
      })
      map
    })

    val by: RDD[((String, String), Iterable[(String, String, Int, Int)])] = map1.groupBy(t=>(t._1,t._2))
    val values: RDD[((String, String), (Int, Int))] = by.mapValues(t => {
      val show: Iterable[Int] = t.map(tp => {
        tp._3
      })


      val impclick: Iterable[Int] = t.map(tp => {
        tp._4
      })

      (show.sum, impclick.sum)
    })

    values.foreach(println)
  }
}
