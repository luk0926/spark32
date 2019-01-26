package cn.edu360.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, types}

/**
  * Created by dell on 2018/6/27.
  */
object DIYSchema {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)

    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    val file: RDD[String] = sc.textFile("product.txt")
    val map: RDD[Array[String]] = file.map(_.split(" "))

    val rowRDD: RDD[Row] = map.map(t=>Row(t(0), t(1).toInt, t(2).toInt))

    val schema: StructType = StructType(
      List(
        StructField("pName", StringType),
        StructField("amount", IntegerType),
        StructField("price", DoubleType)
      ))

    val pDF: DataFrame = sqlContext.createDataFrame(rowRDD, schema)

    pDF.printSchema()

    sc.stop()
  }
}
