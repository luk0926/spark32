package cn.edu360.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/11.
  */
object DataFrameWc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val file: RDD[String] = sc.textFile("wc.txt")

    val split: RDD[String] = file.flatMap(_.split(" "))

    val wcRDD: RDD[Wc] = split.map(Wc(_))

    val wcDF: DataFrame = wcRDD.toDF()

    wcDF.registerTempTable("t_wc")

    sqlContext.sql("select word, count(*) as cnts from t_wc group by word order by cnts desc")
    .show()

    sc.stop()
  }
}
case class Wc(word:String)