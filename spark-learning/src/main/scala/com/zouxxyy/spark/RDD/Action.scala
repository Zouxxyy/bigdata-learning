package com.zouxxyy.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD的行动操作
  */

object Action {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(config)

    val listRDD = sc.makeRDD(List(1, 3, 4, 5, 9, 8), 2)
    val pairRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "aa"), (1, "bb"), (3, "cc"), (3, "dd")),  4)


    /**
      * reduce(func)
      * 惊了，reduceByKey是转换算子，reduce是行动算子
      */

//    println(listRDD.reduce(_ + _))

    /**
      * collect()
      * 收集返回数组
      */

//    listRDD.collect()
//      .foreach(println)

    /**
      * count()
      * 计数
      */

//    println(listRDD.count())

    /**
      * first()
      * 返回第一个
      */

//    println(listRDD.first())

    /**
      * take(n)
      * 取前几个
      */

//    listRDD.take(3)
//      .foreach(println)

    /**
      * takeOrdered(n)
      * 取排序后的前几个
      */

//    listRDD.takeOrdered(3)
//      .foreach(println)

    /**
      * aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
      * seqOp分区内，combOp分区间
      * aggregate分区间也加初始值，aggregateByKey不加
      */

//    println(listRDD.aggregate(10)(_ + _, _ + _))

    /**
      * fold(zeroValue: T)(op: (T, T) => T)
      * aggregate简化
      */

//    println(listRDD.fold(10)(_ + _))

    /**
      * saveAsTextFile(...)
      * saveAsObjectFile(...)
      */

//    listRDD.saveAsTextFile("data/output1")
//    listRDD.saveAsObjectFile("data/output2")


    /**
      * countByKey(): Map[K, Long]
      * 生成的是字典
      */

//    println(pairRDD.countByKey())

    /**
      * foreach(func)
      */

    listRDD.foreach(println)

    sc.stop()

  }
}
