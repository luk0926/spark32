package cn.edu360.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/11.
  */
object DataFrameDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext: SQLContext = new SQLContext(sc)
    //导入隐式转换
    import sqlContext.implicits._

    val file: RDD[String] = sc.textFile("product.txt")

    val split: RDD[Array[String]] = file.map(_.split(" "))

    val pRDD: RDD[Pod] = split.map(t=>Pod(t(0), t(1).toInt, t(2).toInt))

    //将pRDD转换成DataFrame
    val pDF: DataFrame = pRDD.toDF()

    //注册成表
    pDF.registerTempTable("t_product")

    //执行sql语句
    val sql: DataFrame = sqlContext.sql("select * from t_product")

    sql.show()

    sc.stop()
  }
}
case class Pod(name:String, price:Int, amount:Int)