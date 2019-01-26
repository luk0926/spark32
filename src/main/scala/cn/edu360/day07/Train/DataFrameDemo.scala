package cn.edu360.day07.Train

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/12.
  */
object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext: SQLContext = new SQLContext(sc)
    //导入sqlContext的隐式转换
    import sqlContext.implicits._

    //读取文件
    val file: RDD[String] = sc.textFile("product.txt")

    //切割
    val split: RDD[Array[String]] = file.map(_.split(" "))

    val pRDD: RDD[MyProduct] = split.map(t => MyProduct(t(0), t(1).toInt, t(2).toInt))

    val pDF: DataFrame = pRDD.toDF()

    //注册成临时表
    pDF.registerTempTable("t_product")

    sqlContext.sql("select * from t_product")
      .show()


    sc.stop()
  }
}

case class MyProduct(name: String, price: Int, amount: Int)
