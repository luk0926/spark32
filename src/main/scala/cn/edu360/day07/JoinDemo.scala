package cn.edu360.day07

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/11.
  */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()
    import spark.implicits._

    val pro: Dataset[String] = spark.createDataset(List("手机 1500 11", "手表 999 22", "键盘 280 33", "鼠标 89 44"))

    val pDS: Dataset[(String, Int, Int)] = pro.map(t => {
      val split: Array[String] = t.split(" ")
      val name: String = split(0)
      val price: Int = split(1).toInt
      val code: Int = split(2).toInt

      (name, price, code)
    })

    val pDF: DataFrame = pDS.toDF("name", "price", "code")
    pDF.createTempView("pro")

    val pType: Dataset[String] = spark.createDataset(Array("11 打电话", "22 装逼", "33 打字", "44 光标"))

    val typeDS: Dataset[(Int, String)] = pType.map(t => {
      val split: Array[String] = t.split(" ")
      val code: Int = split(0).toInt
      val typ: String = split(1)
      (code, typ)
    })

    val typeDF: DataFrame = typeDS.toDF("code", "tpye")

    typeDF.createTempView("type")


    //多表关联
    spark.sql("select * from pro p join type t on p.code = t.code")
      .show()

    spark.stop()
  }
}
