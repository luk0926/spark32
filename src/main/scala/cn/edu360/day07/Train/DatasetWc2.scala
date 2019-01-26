package cn.edu360.day07.Train

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/12.
  */
object DatasetWc2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()
    //导入spark的隐式转换
    import spark.implicits._

    //读取文件
    val file: Dataset[String] = spark.read.textFile("wc.txt")

    //切割并压平
    val split: Dataset[String] = file.flatMap(_.split(" "))

    //自定义schema
    val wcDF: DataFrame = split.toDF("word")

    //注册成临时表
    wcDF.createTempView("t_wc")

    //执行sql查询语句
    spark.sql("select word,count(*) cnts from t_wc group by word order by cnts desc")
      .show()

    spark.stop()
  }
}
