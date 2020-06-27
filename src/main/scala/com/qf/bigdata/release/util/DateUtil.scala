package com.qf.bigdata.release.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
  * @Author liaojincheng
  * @Date 2020/6/26 11:46
  * @Version 1.0
  * @Description
  * 数据处理工具类
  */
object DateUtil {
  //时间参数转换  time => String
  def dateFormat4String(bdp_day_begin:String, str:String):String = {
    if(null == bdp_day_begin) {
      return null
    }
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(str)
    val datetime: LocalDate = LocalDate.parse(bdp_day_begin, formatter)
    val str1: String = datetime.format(DateTimeFormatter.ofPattern(str))
    str1
  }

  //计算时间差
  def dateFormat4StringDiff(date:String, formatter:String="yyyyMMdd"):String ={
    if(date == null){
      return null
    }
    //1.将formatter格式 转化成 DateTimeFormatter类型
    val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern(formatter)
    //2.将date时间按照formatter格式转化成LocalDate类型
    val localDate = LocalDate.parse(date, formatter1)
    //3.使用plus方法 +1天
    val newDate = localDate.plusDays(1)
    val str = newDate.format(DateTimeFormatter.ofPattern(formatter))
    str
  }

}
