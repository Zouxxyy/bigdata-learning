package com.zouxxyy.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 3种缓存方式
  */

object Cache {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Cache")
    val sc = new SparkContext(sparkConf)

    val listRDD = sc.makeRDD(List(1, 2))

    // cache() 和 persist() 默认保存在内存中
    val noCache = listRDD.map(_.toString + System.currentTimeMillis())

    noCache.collect().foreach(a => println(a))

    noCache.collect().foreach(a => println(a)) // 每次执行都是从头开始，所以时间戳不同

    val cache = listRDD.map(_.toString + System.currentTimeMillis()).cache()

    cache.collect().foreach(a => println(a))

    cache.collect().foreach(a => println(a)) // 从cache开始，时间戳相同


    // checkpoint(),会将指定结果保存在指定目录

    // 设定检查点点保存目录
    sc.setCheckpointDir("output")

    val mapRDD: RDD[(Int, Int)] = listRDD.map((_, 1))

    mapRDD.checkpoint()

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.foreach(println) // 执行行动操作后checkpoint才生效

    println(reduceRDD.toDebugString)


    sc.stop()

  }

}
