package com.zouxxyy.spark.RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计出每一个省份广告被点击次数的TOP3
  * 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割
  */

object Agent {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Agent")
    val sc = new SparkContext(sparkConf)

    val line = sc.textFile("data/input/agent")

    // map: ((省份, 广告),1)
    val provinceAdToOne = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }

    // reduceByKey : ((省份, 广告), sum)
    val provinceAdToSum = provinceAdToOne.reduceByKey(_ + _)

    // map : (省份, (广告, sum))
    val provinceToAdSum = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2)))

    // groupByKey: (省份 ,List((广告1, sum1),(广告2, sum2)...))
    val provinceGroup = provinceToAdSum.groupByKey()

    // 排序并取前3条 (sortWith是scala中的方法)
    val provinceAdTop3 = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }

    provinceAdTop3.collect().foreach(println)

    sc.stop()

  }
}
