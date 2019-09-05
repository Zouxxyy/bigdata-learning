package com.zouxxyy.spark.chap14

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

/**
  * 第14章：电商交互式分析系统应用案例
  */

object EB_Users_Analyzer_DateSet {

  case class UserLog(logID: Long, userID: Long, time: String, typed: Long, consumed: Double)

  case class LogOnce(logID: Long, userID: Long, count: Long)

  case class ConsumedOnce(logID: Long, userID: Long, consumed: Double)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("EB_Users_Analyzer_DateSet").master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._


    // 先读取数据
    // 方式1：json读取
    val userInfoJson = spark.read.format("json").json("data/chap14/sql/user.json")
    val userLogJson = spark.read.format("json").json("data/chap14/sql/log.json")
    userInfoJson.printSchema()
    userLogJson.printSchema()

    // 方式2：parquet读取
    val userInfo = spark.read.format("parquet").parquet("data/chap14/sql/userparquet.parquet")
    val userLog:DataFrame = spark.read.format("parquet").parquet("data/chap14/sql/logparquet.parquet")
    userInfo.printSchema()
    userLog.printSchema()

    /**
      root
       |-- name: string (nullable = true)
       |-- registeredTime: string (nullable = true)
       |-- userID: long (nullable = true)

      root
       |-- _corrupt_record: string (nullable = true)
       |-- consumed: double (nullable = true)
       |-- logID: long (nullable = true)
       |-- time: string (nullable = true)
       |-- typed: long (nullable = true)
       |-- userID: long (nullable = true)
      */

    /**
      * 功能一：特定时间段内用户访问电商网站排名TopN:
      * 先筛选在这时间内且typed=0的数据，再用groupBy(userID)和count，计算出个数
      */

    val startTime = "2016-10-01"
    val endTime = "2016-11-01"

    userLog.filter("time >= '" + startTime + "' and time <= '" + endTime + "' and typed = 0")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userLog("logID")).alias("userLogCount"))
      .sort($"userLogCount".desc)
      .limit(5).show()

    /**
      +------+-------+------------+
      |userID|   name|userLogCount|
      +------+-------+------------+
      |    39|spark39|          45|
      |    11|spark11|          39|
      |     9| spark9|          39|
      |    45|spark45|          38|
      |    25|spark25|          38|
      +------+-------+------------+
      */

    /**
      * 功能二：统计特定时间段内用户购买总金额排名TopN
      */

    userLog.filter("time >= '" + startTime + "' and time <= '" + endTime + "' and typed = 1")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLog("consumed")), 2).alias("totalCount")) //round函数用于四舍五入
      .sort($"totalCount".desc)
      .limit(5).show()

    /**
      +------+-------+----------+
      |userID|   name|totalCount|
      +------+-------+----------+
      |    92|spark92|   20109.1|
      |    14|spark14|  19991.64|
      |    40|spark40|  19098.22|
      |    64|spark64|  19095.71|
      |    46|spark46|  18895.19|
      +------+-------+----------+
      */

    /**
      * 功能三：统计特定时间段内用户访问次数增长排名TopN（使用DataSet）
      * 在 userAccessTemp 中 count的值 1 表示本周的记录 ，-1  表示上周的记录，sum求和就正负抵消巧妙地统计出同一用户本周比上周增长的访问次数
      */

    val userAccessTemp = userLog.as[UserLog].filter("time >= '2016-10-08' and time <= '2016-10-14' and typed = '0'")
      .map(log => LogOnce(log.logID, log.userID, 1))
      .union(userLog.as[UserLog].filter("time >= '2016-10-01' and time <= '2016-10-07' and typed = '0'")
        .map(log => LogOnce(log.logID, log.userID, -1))) // (logID, userID, count)

    userAccessTemp.join(userInfo, userInfo("userID") === userAccessTemp("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userAccessTemp("count")), 2).alias("viewIncreasedTmp"))
      .sort($"viewIncreasedTmp".desc)
      .limit(10)
      .show(10)

    /**
      +------+-------+----------------+
      |userID|   name|viewIncreasedTmp|
      +------+-------+----------------+
      |     8| spark8|              10|
      |    52|spark52|               9|
      |    28|spark28|               7|
      |    78|spark78|               7|
      |    75|spark75|               7|
      |    20|spark20|               7|
      |    34|spark34|               6|
      |    88|spark88|               6|
      |    66|spark66|               6|
      |    85|spark85|               6|
      +------+-------+----------------+
      */

    /**
      * 功能四：统计特定时段购买金额增长最多的 Top5 用户
      * 和功能三做法类似，把count改成log.consumed
      */

    val userLogConsumerDS = userLog.as[UserLog].filter("time >= '2016-10-08' and time <= '2016-10-14' and typed = '1'")
      .map(log => ConsumedOnce(log.logID, log.userID, log.consumed))
      .union(userLog.as[UserLog].filter("time >= '2016-10-01' and time <= '2016-10-07' and typed = '1'")
        .map(log => ConsumedOnce(log.logID, log.userID, -log.consumed))) // (logID, userID, consumed)

    userLogConsumerDS.join(userInfo, userInfo("userID") === userLogConsumerDS("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLogConsumerDS("consumed")), 2).alias("viewConsumedIncreased"))
      .sort($"viewConsumedIncreased".desc)
      .limit(10)
      .show(10)

    /**
      +------+-------+---------------------+
      |userID|   name|viewConsumedIncreased|
      +------+-------+---------------------+
      |    47|spark47|              6684.31|
      |    62|spark62|               5319.8|
      |    83|spark83|              5282.87|
      |    45|spark45|              4677.08|
      |    11|spark11|              4480.73|
      |    78|spark78|              4230.76|
      |    82|spark82|               4184.6|
      |    90|spark90|               4134.5|
      |    41|spark41|              3951.47|
      |    91|spark91|               3909.9|
      +------+-------+---------------------+
      */

    /**
      * 功能五：统计特定时段注册之后前两周内访问次数最多的 Top10 用户
      */

    userLog.join(userInfo, userInfo("userID") === userLog("userID"))
      .filter(userInfo("registeredTime") >= "2016-10-01"
          && userInfo("registeredTime") <= "2016-10-14" // 特定时段注册
          && userLog("time") >= userInfo("registeredTime")
          && userLog("time") <= date_add(userInfo("registeredTime"), 14) // 统计特定时段注册之后前两周内
          && userLog("typed") === 0)
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userLog("logID")).alias("logTimes"))
      .sort($"logTimes".desc)
      .limit(10)
      .show()

    /**
      +------+-------+--------+
      |userID|   name|logTimes|
      +------+-------+--------+
      |    11|spark11|      29|
      |    66|spark66|      26|
      |     8| spark8|      25|
      |    92|spark92|      25|
      |    63|spark63|      24|
      |     4| spark4|      24|
      |     9| spark9|      24|
      |    96|spark96|      23|
      |    88|spark88|      23|
      |    37|spark37|      23|
      +------+-------+--------+
      */

    /**
      * 功能六：统计注册之后前两周内购买总额最多的Top 10 用户
      * 和功能五方法基本一样
      */

    userLog.join(userInfo, userInfo("userID") === userLog("userID"))
      .filter(userInfo("registeredTime") >= "2016-10-01"
        && userInfo("registeredTime") <= "2016-10-14" // 特定时段注册
        && userLog("time") >= userInfo("registeredTime")
        && userLog("time") <= date_add(userInfo("registeredTime"), 14) // 统计特定时段注册之后前两周内
        && userLog("typed") === 1)
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLog("consumed")), 2).alias("totalConsumed"))
      .sort($"totalConsumed".desc)
      .limit(10)
      .show()

    /**
      +------+-------+-------------+
      |userID|   name|totalConsumed|
      +------+-------+-------------+
      |    92|spark92|     15555.91|
      |    65|spark65|     15191.59|
      |    59|spark59|     14238.02|
      |    40|spark40|     14176.72|
      |    80|spark80|     13328.77|
      |    91|spark91|     13245.58|
      |    57|spark57|     12997.61|
      |    64|spark64|     12717.84|
      |    26|spark26|     11667.84|
      |     5| spark5|     11608.82|
      +------+-------+-------------+
      */

//    while(true){}

    spark.stop()
  }
}
