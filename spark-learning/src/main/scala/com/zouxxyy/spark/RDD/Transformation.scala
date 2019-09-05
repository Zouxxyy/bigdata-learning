package com.zouxxyy.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * RDD的转换操作，分三种：单value，双value交互，（k,v）对
  */

object Transformation {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transformation")

    val sc = new SparkContext(config)

    val listRDD = sc.makeRDD(1 to 10)
    val listRDD2 = sc.makeRDD(Array(List(1, 2), List(3, 4)))
    val listRDD3 = sc.makeRDD(5 to 14)

    /***************************单value*****************************/

    /**
      * map(func)
      * 每次处理1条数据
      */

//    val mapRDD = listRDD.map(_ * 2)

    /**
      * mapPartitions(func)
      * 每次处理一组分区数据，效率高，但可能出现内存溢出（因为处理完一组分区后再释放）
      */

//     val mapPartitionsRDD = listRDD.mapPartitions(datas=>{
//       datas.map(data => data * 2)
//     })


    /**
      * mapPartitionsWithIndex(func)
      * 函数的输入多了分区号
      */

//    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
//      case (num, datas) => {
//        datas.map((_, " 分区号：" + num))
//      }
//    }

    /**
      *  flatMap(func)
      *  将map后的数据扁平
      */

//    val flatMAPRDD: RDD[Int] = listRDD2.flatMap(datas => datas)

    /**
      * glom()
      * 将一个分区的数据放在一个数组里
      */

//    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    /**
      * groupBy(func)
      * 按照函数的返回值进行分组，分组后的数据（K：分组的key,V：分组的集合）
      */

//    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i%2)
//    groupByRDD.collect().foreach(println)

    /**
      * filter(func)
      * 按照返回值为true的过滤
      */

//    val filterRDD: RDD[Int] = listRDD.filter(x => x % 2 ==0)
//    filterRDD.collect().foreach(println)

    /**
      * sample(withReplacement : scala.Boolean, fraction : scala.Double, seed : scala.Long)
      * 随机抽样
      */

//    val sampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)
//    sampleRDD.collect().foreach(println)

    /**
      * distinct()
      * 去重，且去重后会shuffler，可以指定去重后的分区数
      */

//    val distinctRDD: RDD[Int] = listRDD.distinct()
//    distinctRDD.collect().foreach(println)

    /**
      * coalesce(n)
      * 缩减分区的数量，可以简单的理解为合并分区，默认，没有shuffler，可以加参数true指定shuffler
      */

//    println("缩减分区前 = " + listRDD.partitions.size)
//    val coalesceRDD: RDD[Int] = listRDD.coalesce(2)
//    println("缩减分区前 = " + coalesceRDD.partitions.size)

    /**
      * repartition()
      * 重新分区，有shuffler。它其实就是带true的coalesce
      */

//    listRDD.glom().collect().foreach(arrays => {
//      println(arrays.mkString(","))
//    })
//    val repartitionRDD: RDD[Int] = listRDD.repartition(2)
//    repartitionRDD.glom().collect().foreach(arrays => {
//      println(arrays.mkString(","))
//    })

    /**
      * sortBy(f: (T) => K,ascending: Boolean = true,numPartitions: Int = this.partitions.length))
      * 根据函数排序
      */

//    val sortByRDD: RDD[Int] = listRDD.sortBy(n => n % 2, false)
//    sortByRDD.collect().foreach(println)

    /**************************双value交互*****************************/

    /**
      * 双value交互
      * A.union(B)         对A、B合并。（不去重）
      * A.subtract(B)      对A减去和B中的相同的
      * A.cartesian(B)     对A、B求笛卡尔乘积
      * A.zip(B)           将A、B组成(k,v)，个数、分区数要相等
      * A.union(B) 对A、B求并集
      */

//    listRDD.union(listRDD3).collect().foreach(println)
//    listRDD.subtract(listRDD3).collect().foreach(println)
//    listRDD.intersection(listRDD3).collect().foreach(println)
//    listRDD.cartesian(listRDD3).collect().foreach(println)
//    listRDD.zip(listRDD3).collect().foreach(println)


    /**************************（k,v）对*******************************/

    val pairRDD1: RDD[(Int, String)] = sc.parallelize(Array((1, "aa"), (1, "bb"), (3, "cc"), (3, "dd")),  4)
    val pairRDD2: RDD[(String, Int)] = sc.parallelize(Array(("a", 3), ("a", 2), ("c", 4),
                                                            ("b", 3), ("c", 6), ("c", 8)),  2)
    val pairRDD3: RDD[(Int, String)] = sc.parallelize(Array((1, "zzz"), (3, "xxx")))

    /**
      * partitionBy(partitioner: Partitioner)
      * 按照分区器进行分区
      */

//    pairRDD1.partitionBy(new org.apache.spark.HashPartitioner(2))
//      .glom().collect().foreach(arrays => {
//      println(arrays.mkString(","))
//    })

//    pairRDD1.partitionBy(new MyPartitioner(3))
//      .glom().collect().foreach(arrays => {
//      println(arrays.mkString(","))
//    })

    /**
      * groupByKey()
      * 单纯把key相等的value放在一起，生成序列
      */
//    pairRDD1.groupByKey().collect().foreach(println)


    /**
      * reduceByKey(func)
      * 按key聚合，并且按函数对key相等的value进行操作
      */

//    pairRDD1.reduceByKey(_ + _)
//      .glom().collect().foreach(arrays => {
//      println(arrays.mkString(","))
//    })


    /**
      * aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U)
      * zeroValue：每个分区的每一个key的初始值
      * seqOp：每个分区里的聚合函数
      * seqOp：分区间的聚合函数
      */


    // 取出每个分区相同对key的最大值，在相加
//    pairRDD2.aggregateByKey(0)(math.max(_,_), _+_)
//      .glom().collect().foreach(arrays => {
//      println(arrays.mkString(","))
//    })

    /**
      * foldByKey(zeroValue: V)(func: (V, V) => V)
      * 其实就是aggregateByKey的简化版，seqOp和seqOp相同
      */

//    pairRDD2.foldByKey(0)(_ + _)
//      .glom().collect().foreach(arrays => {
//      println(arrays.mkString(","))
//    })

    /**
      * combineByKey[C](
      * createCombiner: V => C,
      * mergeValue: (C, V) => C,
      * mergeCombiners: (C, C) => C,
      * partitioner: Partitioner,
      * mapSideCombine: Boolean = true,
      * serializer: Serializer = null)
      *
      * 主要就是比aggregateByKey多了一个createCombiner，用于计算初始值
      */

    // 计算相同key的value的均值
//    pairRDD2.combineByKey(
//      (_, 1),
//      (acc:(Int, Int), v) => (acc._1 + v, acc._2 + 1),
//      (acc1:(Int, Int), acc2:(Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
//      .map{case (key, value) => (key, value._1 / value._2.toDouble)}
//      .collect().foreach(println)

    /**
      * sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      * 按key排序
      */

//    pairRDD1.sortByKey(true)
//      .collect().foreach(println)


    /**
      * mapValues(func)
      * 只对value做转换
      */

//    pairRDD1.mapValues(value => value + "|||")
//      .collect().foreach(println)

    /**
      * A.join(B, numP)
      * 把key相同的value组合在一起(性能较低)
      */

//    pairRDD1.join(pairRDD3)
//      .collect().foreach(println)

    /**
      * A.cogroup(B)
      * （k, v1） 和 （k, v2）cogroup 后，得到（k, v1集合，v2集合）
      */

    pairRDD1.cogroup(pairRDD3)
      .collect().foreach(println)

    sc.stop()

  }
}

// 自定义分区器
class MyPartitioner (partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1
  }
}
