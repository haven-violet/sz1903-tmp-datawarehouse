package com.qf.bigdata.release.constant

import org.apache.spark.sql.SaveMode

/**
  * @Author liaojincheng
  * @Date 2020/6/25 11:42
  * @Version 1.0
  * @Description
  *             常量
  */
object ReleaseConstant {

  val SAVEMODE: SaveMode = SaveMode.Overwrite

  //*****列*****
  val COL_RELEASE_SESSION_STATUS = "release_status"

  //*****分区*****
  val DEF_PARTITION = "bdp_day"
  val DEF_SOURCE_PARTITIONS = 4

  //*****表*****
  val ODS_RELEASE_SESSION = "ods_release1903.ods_01_release_session"
  val DW_RELEASE_CUSTOMER = "dw_release1903.dw_release_customer"

}
