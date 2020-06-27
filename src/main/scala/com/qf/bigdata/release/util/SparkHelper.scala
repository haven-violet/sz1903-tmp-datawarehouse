package com.qf.bigdata.release.util

import com.qf.bigdata.release.dm.DMReleaseColumnsHelper
import com.qf.bigdata.release.udf.QFudf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * @Author liaojincheng
  * @Date 2020/6/25 9:50
  * @Version 1.0
  * @Description
  * spark工具类
  */
object SparkHelper {

  /**
    * 写入hive表
    * @param df
    * @param dw
    * @param sm
    * @return
    */
  def writeTableData(df: Dataset[Row], dw: String, sm: SaveMode) = {
    df.write.mode(sm).insertInto(dw)
  }

  /**
    * 获取hive中数据,读取hive表
    * @param spark
    * @param ods
    * @param customerColumns
    */
  def readTableData(spark: SparkSession, ods: String, customerColumns: ArrayBuffer[String]) = {
    //获取hive表数据
    //sss 因为是String*  可变参数, 所以不能直接传入数组,只能通过_* 来传入数组
    val df = spark.read.table(ods).selectExpr(customerColumns: _*)
    df
  }

  def readTableData(spark: SparkSession, ods: String) = {
    //获取hive表数据
    val df = spark.read.table(ods)
    df
  }


  /**
    * 创建SparkSession
    * @param conf
    */
  def createSpark(conf: SparkConf) = {
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark.udf.register("getAgeRange", QFudf.getAgeRange _)
    spark
  }

  /**
    * 参数校验
    * @param bdp_day_begin
    * @param bdp_day_end
    */
  def rangeDates(bdp_day_begin:String, bdp_day_end:String) = {
    val bdp_days = new ArrayBuffer[String]()
    try{
      val begin = DateUtil.dateFormat4String(bdp_day_begin, "yyyyMMdd")
      val end = DateUtil.dateFormat4String(bdp_day_end, "yyyyMMdd")
      if(begin.equals(end)){//传入的开始时间和结束时间相等
        bdp_days.+=(begin)
      }else{//传入的开始时间和结束时间不相等
        var cday = begin
        while(cday!=end){
          bdp_days.+=(cday)
          //计算开始时间和结束时间的时间差
          cday = DateUtil.dateFormat4StringDiff(cday)
        }
      }
    }catch {
      case ex:Exception => {
        ex.printStackTrace()
      }
    }
    bdp_days
  }


}
