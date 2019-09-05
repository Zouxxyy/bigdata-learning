package com.zouxxyy.spark.chap03

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 3.8节 通过 WordCount 实战解析 SparkRDD 内部机制
  */

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName ("WordCount")

    val spark= SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext

    val lines = sc.textFile("data/chap02/wordcount/helloSpark.txt", 1)

    val words = lines.flatMap(line => line.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCountsOdered = pairs.reduceByKey(_ + _)
      .map(pair => (pair._2, pair._1))
      .sortByKey(false) // sortByKey前先调换位置
      .map(pair => (pair._2, pair._1))

    wordCountsOdered.collect.foreach(pair => println(pair._1 + " : " + pair._2))

    spark.stop()

    /**
      Hello : 4
      Spark : 2
      Awesome : 1
      Flink : 1
      is : 1
      Scala : 1
      Hadoop : 1
      */

  }
}
