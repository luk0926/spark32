package cn.edu360.Test.Order

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/10.
  */
object OrderTest4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\order\\orders.log")

    //商品和用户id
    val proAndId: RDD[(String, String)] = file.map(t => {
      val split: Array[String] = t.split(" ")
      val id: String = split(0)
      val pro: String = split(3)

      (pro, id)
    })

    val file1: RDD[String] = sc.textFile("F:\\大数据学习资料\\Spark\\spark(32)\\test\\order\\lable.txt")

    //商品和标签
    val proAndLable: RDD[(String, String)] = file1.map(t => {
      val split: Array[String] = t.split(" ")
      val pro: String = split(1)
      val lable: String = split(2)
      (pro, lable)
    })

    val join: RDD[(String, (String, Option[String]))] = proAndId.leftOuterJoin(proAndLable)

    val map: RDD[(String, String, String)] = join.map(t => {
      val pro: String = t._1
      val id: String = t._2._1

      val lable: String = if (t._2._2 == None) {
        ""
      } else {
        t._2._2.get
      }

      (id, pro, lable)
    })


    val result: RDD[(String, String, String)] = map.sortBy(t=>t).filter(_._3 != "")

    //result.foreach(println)

    //将结果写入到mysql中
    //将结果写入到mysql中
    result.foreach(t=>{
      var conn:Connection = null
      var pstm:PreparedStatement = null

      try{
        val url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8"

        conn = DriverManager.getConnection(url, "root", "1234")

        val pstm2 = conn.prepareStatement("create table IF NOT EXISTS order_4(id varchar(20) ,pro varchar(20), lable varchar(20))")

        pstm2.execute()

        val sql = "insert into order_4 values(?,?,?)"

        pstm = conn.prepareStatement(sql)

        pstm.setString(1, t._1)
        pstm.setString(2, t._2)
        pstm.setString(3, t._3)

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
