package cn.edu360.Test.Order

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/10.
  */
object OrderTest3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\order\\orders.log")

    val proAndPrice: RDD[(String, Int)] = file.map(t => {
      val split: Array[String] = t.split(" ")
      val pro: String = split(3)
      val price: Int = split(4).toInt
      (pro, price)
    })

    val key: RDD[(String, Int)] = proAndPrice.reduceByKey(_+_)

    val result: RDD[(String, Int)] = key.sortBy(-_._2)

    //result.foreach(println)

    //将结果写入到mysql中
    result.foreach(t=>{
      var conn:Connection = null
      var pstm:PreparedStatement = null

      try{
        val url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8"

        conn = DriverManager.getConnection(url, "root", "1234")

        val pstm2 = conn.prepareStatement("create table IF NOT EXISTS order_3(pro varchar(20) ,price int)")

        pstm2.execute()

        val sql = "insert into order_3 values(?,?)"

        pstm = conn.prepareStatement(sql)

        pstm.setString(1, t._1)
        pstm.setInt(2, t._2)

        pstm.execute()
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if(pstm != null){
          pstm.close()
        }

        if(conn != null){
          conn.close()
        }
      }
    })


    sc.stop()
  }
}
