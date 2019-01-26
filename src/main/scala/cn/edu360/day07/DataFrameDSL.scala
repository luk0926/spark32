package cn.edu360.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/11.
  */
object DataFrameDSL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val file: RDD[String] = sc.textFile("product.txt")

    val split: RDD[Array[String]] = file.map(_.split(" "))

    val pRDD: RDD[Pod2] = split.map(t=>Pod2(t(0), t(1).toInt, t(2).toInt))

    val pDF: DataFrame = pRDD.toDF()

    //查询
    //val select: DataFrame = pDF.select("name", "price")
    //select.show()

    //过滤
    //pDF.where("amount > 20")
    //.show()

    //排序  默认是升序
    pDF.sort($"price" desc)
    .show()

    sc.stop()
  }
}
case class Pod2(name:String, price:Int, amount:Int)