package com.zouxxyy.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 序列化测试
  */

object Serializable {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serializable")
    val sc = new SparkContext(config)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "zxy"))

    val search = new Search("h")

    // search 的 isMatch 方法要传给 executor，所以Search要可以序列化
    val match1: RDD[String] = search.getMatch1(rdd)

    match1.collect().foreach(println)

    // 匿名方法中包含query, Search要可以序列化
    val match2: RDD[String] = search.getMatch2(rdd)

    match2.collect().foreach(println)

  }
}


class Search(query:String) extends java.io.Serializable{

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1 (rdd: RDD[String]): RDD[String] = {

    // filter内是成员方法
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {

    // filter内是匿名函数，与对象无关，但query与对象有关
    rdd.filter(x => x.contains(query))

    // 方法2
//    val q = query
//    rdd.filter(x => x.contains(q))
  }

}

