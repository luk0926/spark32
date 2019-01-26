package cn.edu360.day04

import java.util.{Calendar, Date}

/**
  * Created by dell on 2018/6/7.
  */
object DateUtils {
  // 获取日历对象
  private val cal = Calendar.getInstance()
  // 根据字符串获取一个时间类型
  //  def apply(str:String)={
  //    Calendar
  //  }


  // 日期增加的方法
  def dateAdd(dt: Date, day: Int) = {
    cal.setTime(dt)
    cal.add(Calendar.DATE, day)
    cal.getTime
  }

}
