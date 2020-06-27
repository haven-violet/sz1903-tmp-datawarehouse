package com.qf.bigdata.release.dm

import scala.collection.mutable.ArrayBuffer

/**
  * @Author liaojincheng
  * @Date 2020/6/26 23:59
  * @Version 1.0
  * @Description
  */
object DMReleaseColumnsHelper {

  def selectDWReleaseSourceColumns() = {
    val columns = new ArrayBuffer[String]()
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }

  def selectDWCustomerCubeColumns() = {
    val columns = new ArrayBuffer[String]()
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("user_count")
    columns.+=("total_count")
    columns
  }

  def selectDWReleaseCustomerColumns() = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("getAgeRange(age) as age_range")
    columns
  }


}
