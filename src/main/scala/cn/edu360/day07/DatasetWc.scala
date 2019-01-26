package cn.edu360.day07

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/11.
  */
object DatasetWc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()
    //导入隐式转换
    import spark.implicits._

    val file: Dataset[String] = spark.read.textFile("wc.txt")

    val split: Dataset[String] = file.flatMap(_.split(" "))

    val wcDF: DataFrame = split.toDF("word")

    wcDF.createTempView("t_wc")

    spark.sql("select word,count(*) as cnts from t_wc group by word order by cnts desc")
      .show()

    spark.stop()
  }
}
