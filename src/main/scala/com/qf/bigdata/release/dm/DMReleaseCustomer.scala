package com.qf.bigdata.release.dm

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Author liaojincheng
  * @Date 2020/6/26 23:41
  * @Version 1.0
  * @Description
  */
object DMReleaseCustomer {
  //日志处理
  private val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

  /**
    * 目标客户集市
    * @param spark
    * @param appName
    * @param day
    */
  def handleReleaseJob(spark: SparkSession, appName: String, day: String) = {
    try{
      //导入隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //获取日志字段
      val customerColumns = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()
      //设置条件
      val customerCondition: Column = $"${ReleaseConstant.DEF_PARTITION}" === lit(day)
      val dwDF = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER, customerColumns)
        .where(customerCondition)
        //缓存表,因为后面有两个指标统计
        .persist(ReleaseConstant.STORAGE_LEVEL)
      println("===DW DF===")
//      dwDF.show(10, false)
      /*
       * TODO 渠道用户统计
       */
      //设置查询条件

      //sss 获取需要的字段
      val customerSourceColumns = DMReleaseColumnsHelper.selectDWReleaseSourceColumns()

      //sss 设置查询条件
      val customerSourceGroupColumns = Seq[Column](
        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
        $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}"
      )
      val dmDF = dwDF.groupBy(customerSourceGroupColumns: _*)
        .agg(
          countDistinct(ReleaseConstant.COL_RELEASE_DEVICE_NUM).alias(ReleaseConstant.COL_RELEASE_USER_COUNT),
          count(ReleaseConstant.COL_RELEASE_SESSION).alias(ReleaseConstant.COL_RELEASE_TOTAL_COUNT)
        )
        //sss 充当where条件
        .withColumn(s"${ReleaseConstant.DEF_PARTITION}", lit(day))
        .selectExpr(customerSourceColumns: _*)
      //存入hive
//      SparkHelper.writeTableData(dmDF, ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCES, ReleaseConstant.SAVEMODE)
//        dmDF.show(10, false)
      /**
        * TODO 多维统计分析
        */
      //sss 要分组的字段
      val customerCubeGroupColumns = Seq[Column](
        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
        $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
        $"${ReleaseConstant.COL_RELEASE_GENDER}",
        $"${ReleaseConstant.COL_RELEASE_AREA_CODE}",
        $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}"
      )

      //sss 获取需要的字段
      val customerCubeColumns = DMReleaseColumnsHelper.selectDWCustomerCubeColumns()
      val dmDF2 = dwDF.cube(customerCubeGroupColumns: _*)
        .agg(
          countDistinct(ReleaseConstant.COL_RELEASE_DEVICE_NUM).alias(ReleaseConstant.COL_RELEASE_USER_COUNT),
          count(ReleaseConstant.COL_RELEASE_SESSION).alias(ReleaseConstant.COL_RELEASE_TOTAL_COUNT)
        )
      //SSS 充当where条件
        .withColumn(s"${ReleaseConstant.DEF_PARTITION}", lit(day))
        .selectExpr(customerCubeColumns: _*)
      dmDF2.show(10, false)

    }catch {
      case ex:Exception => {
        println("业务处理")
        logger.error(ex.getMessage, ex)
      }
    }
  }

  /**
    * 业务处理
    *
    * @param appName
    * @param bdp_day_begin
    * @param bdp_day_end
    */
  def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String) = {
    var spark:SparkSession = null
    try{
      //配置参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffer.partitions", "32")
        .setAppName(appName)
        .setMaster("local[*]")
      //初始化上下文
      spark = SparkHelper.createSpark(conf)
      //处理时间格式和多天任务
      val days = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      for(day <- days){
        handleReleaseJob(spark, appName, day)
      }
    } catch{
      case ex:Exception => {
        println("程序出错, 退出~~~")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }

  def main(args: Array[String]):Unit ={
    val appName = "dm_release_customer_job"
    val bdp_day_begin = "20200622"
    val bdp_day_end = "20200622"
    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day_begin, bdp_day_end)
    val end = System.currentTimeMillis()
    println(s"appName = [${appName}], begin = ${begin}, use = ${end - begin}")
  }
}
