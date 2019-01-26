package cn.edu360.day07.Train

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/12.
  */
object DataFrameWc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    //创建SQLContext的对象
    val sqlContext: SQLContext = new SQLContext(sc)

    //导入sqlContext的隐式转换
    import sqlContext.implicits._

    //读取文件
    val file: RDD[String] = sc.textFile("wc.txt")

    //切割
    val split: RDD[String] = file.flatMap(_.split(" "))

    //通过样例类来构建schema信息
    val wcRDD: RDD[WordCount] = split.map(WordCount(_))

    //将wcRDD转换成DataFrame类型
    val wcDF: DataFrame = wcRDD.toDF()

    //注册成表
    wcDF.registerTempTable("t_wc")

    //println(wcDF.schema)

    //wcDF.printSchema()

    //执行查询操作
    sqlContext.sql("select word,count(*) as cnts from t_wc group by word order by cnts desc")
      .show()

    sc.stop()
  }
}

//创建样例类，传递 schema信息
case class WordCount(word: String)
