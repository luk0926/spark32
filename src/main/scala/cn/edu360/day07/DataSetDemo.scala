package cn.edu360.day07

import org.apache.spark.network.sasl.SparkSaslServer
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/11.
  */
object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName(this.getClass.getName)
    .getOrCreate()
    //导入隐式转换
    import spark.implicits._

    val file: Dataset[String] = spark.read.textFile("product.txt")

    val split: Dataset[Array[String]] = file.map(_.split(" "))

    val product: Dataset[(String, Int, Int)] = split.map(t=>(t(0), t(1).toInt, t(2).toInt))

    val pDF: DataFrame = product.toDF("name", "price", "amount")

    pDF.createTempView("t_product")

    spark.sql("select * from t_product")
    .show()

    spark.stop()
  }
}
