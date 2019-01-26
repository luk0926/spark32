package cn.edu360.day04

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2018/6/7.
  */
object GameAnalysis_v2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage:cn.edu360.GameAnalysis <input>")
      sys.exit(1)
    }
    // 接收参数
    val Array(input) = args
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // 读取数据   如果数据分割之后，长度不为9  过滤掉非法数据
    val lines: RDD[String] = sc.textFile(input).filter(_.split("\\|").length == 9).cache()

    // 1 过滤出 20160201 数据
    /**
      * 1|2016年9月16日,星期五,23:02:41|192.168.1.102|那谁|武士|男|1|0|0/800000000
      * 字段说明：
      * 日志类型|时间|IP地址|角色名|职业|性别|级别|元宝|金币
      */
    // executor driver: java.text.ParseException (Unparseable date: "2016年02月01日") [duplicate 1]
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
    val startTime = sdf1.parse("2016-02-01")

    val endTime: Date = DateUtils.dateAdd(startTime,1) // 2016 2 2
    val endTime2: Date = DateUtils.dateAdd(startTime,2) // 2016 2 3
    println(endTime,endTime2)

    //  判断 日志类型 为  1
    // 差异  filter 是一个重量级的算子    消耗性能  可以写在一个filter中
    val filterData: RDD[String] = getDayNewUser(lines, startTime, endTime, DataTypes.REGISTER)

    // 1 计算 20160201 新增用户
    val dayNU: Long = filterData.count()
    println(s"20160201注册用户：=${dayNU}")


    // 次日留存
    // 查询 20160202  有多少登录用户  去重
    // 所有的登录用户中，有多少用户是20160201 这一天注册的   100
    // 次日留存率   100*1.0 /  188

    // 第二天上线用户 （登录用户）
    val filterData2 = getDayLoginUser(lines, endTime, endTime2, DataTypes.LOGIN)

    // 根据用户名称来判断 是否是第一天注册的用户
    val registerData = filterData.map(t => {
      t.split("\\|")(3)
    })
    // 登录用户 需要去重
    val loginData = filterData2.map(_.split("\\|")(3)).distinct()

    // 求交集的数量  第一天注册的用户 中  第二天 还在上线的用户
    val secondLoginUser = registerData.intersection(loginData).count()
    //    registerData.zipWithIndex().join(loginData.zipWithIndex()).count()

    println(s"第二天还在登录的用户=${secondLoginUser}")
    println(s"次日留存率；${secondLoginUser * 1.0 / dayNU}")

    sc.stop()
  }

  //  日新增用户
  def getDayNewUser(lines: RDD[String], startTime: Date, endTime: Date, dataType: String): RDD[String] = {
    val filterData: RDD[String] = lines.filter(line => {
      val strs = line.split("\\|")
      // 取出时间字段
      val dateStr = strs(1)
      // 取出类别字段
      val logType = strs(0)
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
      val logDate: Date = sdf.parse(dateStr)

      // 对时间进行比较
      logDate.getTime >= startTime.getTime && logDate.getTime < endTime.getTime && logType.equals(dataType)
    })
    filterData
  }

  // 日 上线用户
  def getDayLoginUser(lines: RDD[String], startTime: Date, endTime: Date, dataType: String): RDD[String] = {
    getDayNewUser(lines, startTime, endTime, dataType)
  }
}


object DataTypes {

  /**
    * 日志类型：
    * 0 管理员登陆
    * 1 首次登陆
    * 2 上线
    * 3 下线
    * 4 升级
    * 5 预留
    * 6 装备回收元宝
    * 7 元宝兑换RMB
    * 8 PK
    * 9 成长任务
    * 10 领取奖励
    * 11 神力护身
    * 12 购买物品
    *
    */
  val ADMIN = "0"
  val REGISTER = "1"
  val LOGIN = "2"
  val LOGOUT = "3"

  // 魔鬼数字
}