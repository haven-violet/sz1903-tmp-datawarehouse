package com.qf.bigdata.release.dw

import org.apache.spark.sql.Column

import scala.collection.mutable.ArrayBuffer

/**
  * @Author liaojincheng
  * @Date 2020/6/25 11:29
  * @Version 1.0
  * @Description
  */
object DWReleaseColumnsHelper {
  def selectDWReleaseCustomerColumns() = {
    var columns = ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts, '$.idcard') as idcard")
    columns.+=("cast(year(current_date()) as int) - cast(regexp_extract(get_json_object(exts, '$.idcard'), '(\\\\d{6})(\\\\d{4})', 2) as int) age")
    columns.+=("cast(regexp_extract(get_json_object(exts, '$.idcard'), '(\\\\d{16})(\\\\d{1})', 2) as int) % 2 as gender")
    columns.+=("get_json_object(exts, '$.area_code') as area_code")
    columns.+=("get_json_object(exts, '$.longitude') as longitude")
    columns.+=("get_json_object(exts, '$.latitude') as latitude")
    columns.+=("get_json_object(exts, '$.matter_id') as matter_id")
    columns.+=("get_json_object(exts, '$.model_code') as model_code")
    columns.+=("get_json_object(exts, '$.model_version') as model_version")
    columns.+=("get_json_object(exts, '$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

}
