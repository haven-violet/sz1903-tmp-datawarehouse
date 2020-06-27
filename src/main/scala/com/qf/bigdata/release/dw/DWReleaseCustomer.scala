package com.qf.bigdata.release.dw

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Author liaojincheng
  * @Date 2020/6/25 9:35
  * @Version 1.0
  * @Description
  */
object DWReleaseCustomer {
  //日志处理
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

  /**
    * 业务处理方法
    * @param spark
    * @param appName
    * @param bdp_day_end
    */
  def handleReleaseJob(spark: SparkSession, appName: String, bdp_day_end: String) = {
    try{
      //导入隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //获取日志字段
      val customerColumns = DWReleaseColumnsHelper.selectDWReleaseCustomerColumns()
      //使用DSL风格编写代码
      //按照条件查询ODS层数据
      val customerReleaseCondition = (
        // sss 在spark中,如果要判断字符串是否相等要使用==, 如果要判断字段Column是否相等要使用===
        // sss
        //  col底层是只能接收String,然后将String类型转化成Column类型,这里可能强转不过来
        //  lit底层是能够接收Any类型,然后将其他类型转化成Column类型,这里它能够强转过来
        //  bdp_day === "20200622"
        col(ReleaseConstant.DEF_PARTITION) === lit(bdp_day_end)
        and
        col(ReleaseConstant.COL_RELEASE_SESSION_STATUS) === lit(ReleaseStatusEnum.CUSTOMER.getCode)
      )
      //获取数据
      val df = SparkHelper.readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION, customerColumns)
        //填入条件
        .where(customerReleaseCondition)
        //如果感觉运行效率有点低,可以扩大并行度
          .repartition(ReleaseConstant.DEF_SOURCE_PARTITIONS)
      //输出打印
//      df.show(10, false)
      //存入hive表中
      SparkHelper.writeTableData(df, ReleaseConstant.DW_RELEASE_CUSTOMER, ReleaseConstant.SAVEMODE)
    }catch {
      case ex:Exception => {
        println("业务处理")
        logger.error(ex.getMessage, ex)
      }
    }
  }

  /**
    * 投放目标客户
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
      //初始化spark上下文
      spark = SparkHelper.createSpark(conf)
      //处理时间格式和多天任务
      val days = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      //处理原始数据(注意: 传入的时间参数还需要处理,等到后面处理)
      for(day <- days){
        handleReleaseJob(spark, appName, day)
      }
    }catch {
      case ex:Exception => {
        println("程序出错, 退出~~~")
        logger.error(ex.getMessage, ex)
      }
    }finally{
      if(spark!=null){
        spark.stop()
      }
    }
  }

  /**
    * 目标客户
    * status = "01"
    */
  def main(args: Array[String]): Unit = {
    val appName = "dw_release_customer_job"
    val bdp_day_begin = "20200622"
    val bdp_day_end = "20200622"
    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day_begin, bdp_day_end)
    val end = System.currentTimeMillis()
    println(s"appName = [${appName}], begin = $begin, use = ${end - begin}")
  }
}
