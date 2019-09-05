package com.zouxxyy.spark.RDD

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * mysql 查询与写入
  */

object Mysql {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Mysql")
    val sc = new SparkContext(sparkConf)

    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/zxy_blog_db"
    val userName = "root"
    val passWd = "elwgelwg"

    // mysql 查询

    val jdbcRDD = new JdbcRDD(sc, () => {

      // 获取数据库连接对象
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select id, ip from log where id>=? and id <= ?",
      1,
      5,
      1,
      rs => {
        println(rs.getString(1) + " , " + rs.getString(2))
      }
    )

    //打印最后结果
    jdbcRDD.collect()

    // mysql 写入

    val dataRDD = sc.makeRDD(List(("0:0:0:0:0:0:0:1"), ("0:0:0:0:0:0:0:1")))


    // 方式1，效率不高
//    dataRDD.foreach {
//      case (ip) => {
//        Class.forName(driver)
//        val connection = DriverManager.getConnection(url, userName, passWd)
//        val sql = "insert into log(ip) values (?)"
//        val statement: PreparedStatement = connection.prepareStatement(sql)
//        statement.setString(1, ip)
//        statement.executeUpdate()
//        statement.close()
//        connection.close()
//      }
//    }

    // 方式2，以分区为单位，一个分区连接一次
    dataRDD.foreachPartition(datas => {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, userName, passWd)
      datas.foreach {
        case (ip) => {
          val sql = "insert into log(ip) values (?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1, ip)
          statement.executeUpdate()
          statement.close()
        }
      }
      connection.close()
    })

    sc.stop()
  }
}
