package com.zouxxyy.spark.RDD

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

/**
  * Accumulator和RDD并列，是spark三大数据结构之一，这里只是放到RDD包里
  */

object Accumulator {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    val sc = new SparkContext(config)

    val dataRDD = sc.makeRDD(List("zxy1", "zxy2", "spark"))

    val wordAccumulator = new WordAccumulator()

    // 注册累加器
    sc.register(wordAccumulator)

    dataRDD.foreach{
      case word => {
        wordAccumulator.add(word)
      }
    }

    println("sum = " + wordAccumulator.value)

    sc.stop()
  }
}

/**
  * 自定义累加器
  * 两个范型分别是输入和输出格式
  * 我们这个累加器，就是添加字符串，返回字符串数组
  */

class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()

  // 判断当前累加器是否是初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  // 拷贝累加器
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  // 初始化累加器
  override def reset(): Unit = {
    list.clear()
  }

  // 向累加器中添加元素
  override def add(v: String): Unit = {
    if(v.contains("zxy")) {
      list.add(v)
    }
  }

  // 两个累加器合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的值
  override def value: util.ArrayList[String] = {
    list
  }
}
