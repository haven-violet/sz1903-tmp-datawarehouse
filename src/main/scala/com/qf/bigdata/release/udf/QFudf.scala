package com.qf.bigdata.release.udf

import org.slf4j.{Logger, LoggerFactory}

/**
  * @Author liaojincheng
  * @Date 2020/6/27 15:54
  * @Version 1.0
  * @Description
  * 自定义函数
  */
object QFudf {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  def getAgeRange(age:String): String = {
    var test = ""
    try {
      if(age != null){
        test =
          if(age.toInt < 18){
            "1"
          }else if(age.toInt >= 18 && age.toInt <= 25){
            "2"
          }else if(age.toInt >=26 && age.toInt <= 35){
            "3"
          }else if(age.toInt >= 36 && age.toInt <= 45){
            "4"
          }else{
            "5"
          }
      }
    }catch {
      case ex:Exception => {
        logger.error(ex.getMessage, ex)
      }
    }
    test
  }
}
