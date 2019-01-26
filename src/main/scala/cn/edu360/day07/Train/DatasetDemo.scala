package cn.edu360.day07.Train

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/12.
  */
object DatasetDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    //导入隐式转换
    import spark.implicits._

    //读取文件
    val file: Dataset[String] = spark.read.textFile("product.txt")

    //切割
    val split: Dataset[Array[String]] = file.map(_.split(" "))

    val pDS = split.map(t => (t(0), t(1).toInt, t(2).toInt))

    //自定义schema
    val pDF: DataFrame = pDS.toDF("name", "price", "amount")

    //注册成临时表
    pDF.createTempView("t_product")

    spark.sql("select * from t_product")
      .show()

    spark.stop()

  }
}
