package com.qf.bigdata.release.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * @Author liaojincheng
  * @Date 2020/6/25 11:42
  * @Version 1.0
  * @Description
  *             常量
  */
object ReleaseConstant {


  //*****SAVEMODE*****
  val STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_ONLY
  val SAVEMODE: SaveMode = SaveMode.Overwrite

  //*****列*****
  val COL_RELEASE_AGE_RANGE = "age_range"
  val COL_RELEASE_AREA_CODE = "area_code"
  val COL_RELEASE_GENDER = "gender"
  val COL_RELEASE_DEVICE_NUM = "device_num"
  val COL_RELEASE_SESSION_STATUS = "release_status"
  val COL_RELEASE_DEVICE_TYPE = "device_type"
  val COL_RELEASE_CHANNELS = "channels"
  val COL_RELEASE_SOURCES = "sources"
  val COL_RELEASE_TOTAL_COUNT: String = "total_count"
  val COL_RELEASE_USER_COUNT: String = "user_count"
  val COL_RELEASE_SESSION = "release_session"

  //*****分区*****
  val DEF_PARTITION = "bdp_day"
  val DEF_SOURCE_PARTITIONS = 4

  //*****表*****
  val DM_RELEASE_CUSTOMER_SOURCES: String = "dm_release1903.dm_customer_sources"
  val ODS_RELEASE_SESSION = "ods_release1903.ods_01_release_session"
  val DW_RELEASE_CUSTOMER = "dw_release1903.dw_release_customer"

}
