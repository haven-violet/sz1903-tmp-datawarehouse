package com.qf.bigdata.release.util

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
    *
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


  /**
    * 创建SparkSession
    *
    * @param conf
    */
  def createSpark(conf: SparkConf) = {
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

}
