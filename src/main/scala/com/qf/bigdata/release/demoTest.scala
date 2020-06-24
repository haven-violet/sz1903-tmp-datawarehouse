package com.qf.bigdata.release

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author liaojincheng
  * @Date 2020/6/24 17:16
  * @Version 1.0
  * @Description
  * 维度立方体(多维分析)
  * group by
  * grouping sets
  * rollup
  * cube
  */
object demoTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("demoTest")
      .master("local")
      .getOrCreate()
    val names = Seq(
      NumberNames("北京", "丰台区", "昨天疫情治愈人数", 10),
      NumberNames("北京", "丰台区", "昨天疫情治愈人数", 20),
      NumberNames("北京", "丰台区", "今天疫情治愈人数", 8 ),
      NumberNames("北京", "丰台区", "前天疫情治愈人数", 15),
      NumberNames("北京", "昌平区", "昨天疫情治愈人数", 11),
      NumberNames("北京", "昌平区", "今天疫情治愈人数", 5 ),
      NumberNames("北京", "昌平区", "前天疫情治愈人数", 20),
      NumberNames("北京", "东城区", "昨天疫情治愈人数", 10),
      NumberNames("北京", "东城区", "今天疫情治愈人数", 1 ),
      NumberNames("北京", "东城区", "今天疫情治愈人数", 5 ),
      NumberNames("北京", "东城区", "昨天疫情治愈人数", 10),
      NumberNames("辽宁", "沈阳区", "昨天疫情治愈人数", 3 ),
      NumberNames("辽宁", "沈阳区", "昨天疫情治愈人数", 2 ),
      NumberNames("辽宁", "沈阳区", "今天疫情治愈人数", 1 )
    )
    import spark.implicits._
    val df: DataFrame = names.toDF
    //注册临时视图
    df.createOrReplaceTempView("numberTmpTable")
/*
+----+----+--------+-----------+
|area|city|     day|sum(counts)|
+----+----+--------+-----------+
|  北京| 丰台区|前天疫情治愈人数|         15|
|  辽宁| 沈阳区|昨天疫情治愈人数|          5|
|  北京| 丰台区|昨天疫情治愈人数|         30|
|  北京| 东城区|昨天疫情治愈人数|         20|
|  北京| 丰台区|今天疫情治愈人数|          8|
|  北京| 昌平区|昨天疫情治愈人数|         11|
|  北京| 昌平区|前天疫情治愈人数|         20|
|  北京| 昌平区|今天疫情治愈人数|          5|
|  北京| 东城区|今天疫情治愈人数|          6|
|  辽宁| 沈阳区|今天疫情治愈人数|          1|
+----+----+--------+-----------+
 */
    /*
    sss group by分组聚合
     */
//    spark.sql(
//      s"""
//         |select
//         |  area,
//         |  city,
//         |  day,
//         |  sum(counts)
//         |from numberTmpTable
//         |group by area,city,day
//       """.stripMargin).show()


    /*
    sss grouping sets是对每一个单独的维度分组取值
      如果写成grouping sets((area, city), (city, day))
      此时相当于做group by(area, city) union group by(city, day)
     */
/*
|area|city|     day|sum(counts)|
+----+----+--------+-----------+
|null|null|前天疫情治愈人数|         35|
|null| 东城区|    null|         26|
|null|null|昨天疫情治愈人数|         66|
|null| 昌平区|    null|         36|
|  辽宁|null|    null|          6|
|null| 丰台区|    null|         53|
|  北京|null|    null|        115|
|null|null|今天疫情治愈人数|         20|
|null| 沈阳区|    null|          6|
+----+----+--------+-----------+

+----+----+--------+-----------+
|area|city|     day|sum(counts)|
+----+----+--------+-----------+
|  北京| 昌平区|    null|         36|
|null| 丰台区|前天疫情治愈人数|         15|
|null| 东城区|昨天疫情治愈人数|         20|
|null| 东城区|今天疫情治愈人数|          6|
|null| 昌平区|前天疫情治愈人数|         20|
|null| 丰台区|今天疫情治愈人数|          8|
|  辽宁| 沈阳区|    null|          6|
|null| 昌平区|昨天疫情治愈人数|         11|
|null| 沈阳区|昨天疫情治愈人数|          5|
|null| 丰台区|昨天疫情治愈人数|         30|
|  北京| 丰台区|    null|         53|
|null| 昌平区|今天疫情治愈人数|          5|
|  北京| 东城区|    null|         26|
|null| 沈阳区|今天疫情治愈人数|          1|
+----+----+--------+-----------+
 */
//    spark.sql(
//      s"""
//         |select
//         |  area,
//         |  city,
//         |  day,
//         |  sum(counts)
//         |from numberTmpTable
//         |group by area,city,day grouping sets (area,city,day)
//       """.stripMargin).show()
//
//    spark.sql(
//      s"""
//         |select
//         |  area,
//         |  city,
//         |  day,
//         |  sum(counts)
//         |from numberTmpTable
//         |group by area,city,day grouping sets ((area,city), (city,day))
//       """.stripMargin).show()

    /*
    sss rollup
      是根据维度在数据结果集中进行的聚合操作
      group by A,B,C with rollup 首先会对(A,B,C)进行group by, 然后在对(A, B)进行group by
      然后在对(A)进行group by最后在进行全表分组操作
      (A,B,C)
      (A,B)
      (A)
      ()
     */
/*
|area|city|     day|total|
+----+----+--------+-----+
|  北京| 昌平区|    null|   36|
|  北京| 昌平区|昨天疫情治愈人数|   11|
|  辽宁| 沈阳区|今天疫情治愈人数|    1|
|  北京| 东城区|昨天疫情治愈人数|   20|
|  北京| 东城区|今天疫情治愈人数|    6|
|  北京| 丰台区|昨天疫情治愈人数|   30|
|  北京| 昌平区|前天疫情治愈人数|   20|
|null|null|    null|  121|
|  辽宁| 沈阳区|    null|    6|
|  辽宁|null|    null|    6|
|  辽宁| 沈阳区|昨天疫情治愈人数|    5|
|  北京| 丰台区|今天疫情治愈人数|    8|
|  北京|null|    null|  115|
|  北京| 丰台区|前天疫情治愈人数|   15|
|  北京| 昌平区|今天疫情治愈人数|    5|
|  北京| 丰台区|    null|   53|
|  北京| 东城区|    null|   26|
+----+----+--------+-----+
 */
//    spark.sql(
//      s"""
//         |select
//         |  area,
//         |  city,
//         |  day,
//         |  sum(counts) as total
//         |from numberTmpTable
//         |group by area,city,day with rollup
//       """.stripMargin).show()

    /*
    sss cube
      在分组基础上再次分组操作group by A,B,C 首先进行(A,B),(A,C),(A),(B,C),(B),(C)
      (A,B,C)
      (A,B)
      (A,C)
      (B,C)
      (A)
      (B)
      (C)
      ()
     */
    spark.sql(
      s"""
         |select
         |  area,
         |  city,
         |  day,
         |  sum(counts)
         |from tab1 group by area,city,day with cube
       """.stripMargin).show()



  }
}

case class NumberNames(area:String, city:String, day:String, counts:Int)