package cn.edu360.day08

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by dell on 2018/6/12.
  */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    //导入spark的隐式转换
    import spark.implicits._

    val pro: Dataset[String] = spark.createDataset(List("手机 1999 11", "卷纸 5 22", "水杯 50 33", "键盘 180 44"))

    val pDS: Dataset[(String, Int, Int)] = pro.map(t => {
      val split: Array[String] = t.split(" ")
      val product: String = split(0)
      val price: Int = split(1).toInt
      val code: Int = split(2).toInt

      (product, price, code)
    })

    val pDF: DataFrame = pDS.toDF("product", "price", "code")

    //注册成临时表
    //pDF.createTempView("t_product")


    val pType: Dataset[String] = spark.createDataset(Array("11 科技", "22 日用品", "33 生活用品", "44 外设"))

    val typDS: Dataset[(Int, String)] = pType.map(t => {
      val split: Array[String] = t.split(" ")
      val code: Int = split(0).toInt
      val typ: String = split(1)

      (code, typ)
    })

    val typDF: DataFrame = typDS.toDF("code", "type")

    //注册成临时表
    //typDF.createTempView("t_type")

    //join关联多张表
    spark.sql("select * from t_product pro join t_type typ on pro.code = typ.code")
    .show()


    //DSL风格


    spark.stop()
  }
}
