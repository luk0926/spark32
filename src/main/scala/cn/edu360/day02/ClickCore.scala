package cn.edu360.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/8/12.
  *
  * 展示量和点击量使用spark core来实现
  */
object ClickCore {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //读取文件
    val file: RDD[String] = sc.textFile("E:\\大数据学习资料\\spark\\spark-02\\作业\\impclick\\impclick.txt")

    val map: RDD[(String, String, Int, Int)] = file.flatMap(t => {
      val split: Array[String] = t.split(",")

      val id: String = split(0)
      val keyWord: String = split(1)
      val show: Int = split(2).toInt
      val click: Int = split(3).toInt

      val split1: Array[String] = keyWord.split("\\|")
      val tuples: Array[(String, String, Int, Int)] = split1.map(t => (id, t, show, click))

      tuples
    })

    //求展示量和点击量
    val map1: RDD[((String, String), (Int, Int))] = map.map(t => {
      ((t._1, t._2), (t._3, t._4))
    })

    val re: RDD[((String, String), (Int, Int))] = map1.reduceByKey((a, b)=> (a._1+b._1, a._2+b._2))

    re.foreach(println)

    sc.stop()
  }
}
