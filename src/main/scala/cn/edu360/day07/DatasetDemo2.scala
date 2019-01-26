package cn.edu360.day07

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/11.
  */
object DatasetDemo2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()
    //导入隐式转换
    import spark.implicits._

    //读取文件
    val file: Dataset[String] = spark.read.textFile("product.txt")

    val split: Dataset[Array[String]] = file.map(_.split(" "))

    val product: Dataset[(String, Int, Int)] = split.map(t => (t(0), t(1).toInt, t(2).toInt))

    val pDF: DataFrame = product.toDF("name", "price", "amount")

    //pDF.select("name", "price")

    //pDF.filter("price > 100")

    pDF.sort($"amount" desc)
      .show()

    spark.stop()
  }
}
