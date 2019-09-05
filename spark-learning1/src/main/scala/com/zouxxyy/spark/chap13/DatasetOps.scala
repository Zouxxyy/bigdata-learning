package com.zouxxyy.spark.chap13

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 第13章：Dataset开发实战企业人员管理系统应用案例
  */

object DatasetOps {

  case class Person(name: String, age: Long)
  case class Score(n: String, score: Long)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("DatasetOps").master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._


    val persons = spark.read.json("data/chap13/peoplemanagedata/people.json")
    val personsDS = persons.as[Person]
    personsDS.show()

    /**
      +---+-------+
      |age|   name|
      +---+-------+
      | 16|Michael|
      | 30|   Andy|
      | 19| Justin|
      | 29| Justin|
      | 46|Michael|
      +---+-------+
      */

    val personsScores = spark.read.json("data/chap13/peoplemanagedata/peopleScores.json")
    val personsScoresDS = personsScores.as[Score]
    personsScoresDS.show()

    /**
      +-------+-----+
      |      n|score|
      +-------+-----+
      |Michael|   88|
      |   Andy|  100|
      | Justin|   89|
      +-------+-----+
      */

    // map算子解析
    personsDS.map(person => (person.name, person.age + 100L)).show()


    /**
      +-------+---+
      |     _1| _2|
      +-------+---+
      |Michael|116|
      |   Andy|130|
      | Justin|119|
      | Justin|129|
      |Michael|146|
      +-------+---+
      */

    // flatMap算子解析
    personsDS.flatMap(persons => persons match {
      case Person(name, age) if (name == "Andy") => List((name, age + 70))
      case Person(name, age) => List((name, age + 30))
    }).show()

    /**
      +-------+---+
      |     _1| _2|
      +-------+---+
      |Michael| 46|
      |   Andy|100|
      | Justin| 49|
      | Justin| 59|
      |Michael| 76|
      +-------+---+
      */

    // mapPartitions算子解析
    personsDS.mapPartitions {
      persons => val result = ArrayBuffer[(String, Long)]()
        while (persons.hasNext) {
          val person = persons.next()
          result += ((person.name, person.age + 1000))
        }
        result.iterator
    }.show

    /**
      +-------+----+
      |     _1|  _2|
      +-------+----+
      |Michael|1016|
      |   Andy|1030|
      | Justin|1019|
      | Justin|1029|
      |Michael|1046|
      +-------+----+
      */

    // dropDuplicate算子解析
    personsDS.dropDuplicates("name").show() // 删除重复的name,(只比较name)

    /**
      +---+-------+
      |age|   name|
      +---+-------+
      | 16|Michael|
      | 30|   Andy|
      | 19| Justin|
      +---+-------+
      */

    // distinct算子解析
    personsDS.distinct().show() // 完全相同再删除

    /**
      +---+-------+
      |age|   name|
      +---+-------+
      | 46|Michael|
      | 30|   Andy|
      | 19| Justin|
      | 16|Michael|
      | 29| Justin|
      +---+-------+
      */

    // repartition算子解析（小分区变大分区）
    println("原分区数：" + personsDS.rdd.partitions.size)
    println("repartition设置分区数：" + personsDS.repartition(4).rdd.partitions.size)

    // coalesce算子解析 （大分区变小区）
    println("coalesce设置分区数：" + personsDS.coalesce(2).rdd.partitions.size)


    /**
      原分区数：1
      repartition设置分区数：4
      coalesce设置分区数：1
      */

    // sort算子解析
    personsDS.sort($"age".desc).show

    /**
      +---+-------+
      |age|   name|
      +---+-------+
      | 46|Michael|
      | 30|   Andy|
      | 29| Justin|
      | 19| Justin|
      | 16|Michael|
      +---+-------+
      */

    // join算子解析
    personsDS.join(personsScoresDS, $"name" === $"n").show

    /**
      +---+-------+-------+-----+
      |age|   name|      n|score|
      +---+-------+-------+-----+
      | 16|Michael|Michael|   88|
      | 30|   Andy|   Andy|  100|
      | 19| Justin| Justin|   89|
      | 29| Justin| Justin|   89|
      | 46|Michael|Michael|   88|
      +---+-------+-------+-----+
      */

    // joinWith算子解析
    personsDS.joinWith(personsScoresDS, $"name" === $"n").show

    /**
      +-------------+-------------+
      |           _1|           _2|
      +-------------+-------------+
      |[16, Michael]|[Michael, 88]|
      |   [30, Andy]|  [Andy, 100]|
      | [19, Justin]| [Justin, 89]|
      | [29, Justin]| [Justin, 89]|
      |[46, Michael]|[Michael, 88]|
      +-------------+-------------+
      */

    // randomSplit算子解析
    personsDS.randomSplit(Array(10, 20)).foreach(dataset => dataset.show) // 按照权重10,20随机划分到两个Dataset[Person]

    /**
      +---+------+
      |age|  name|
      +---+------+
      | 29|Justin|
      +---+------+

      +---+-------+
      |age|   name|
      +---+-------+
      | 16|Michael|
      | 19| Justin|
      | 30|   Andy|
      | 46|Michael|
      +---+-------+
      */

    // sample算子解析
    personsDS.sample(false, 0.5).show() // 无放回随机采样，采样率为0.5

    /**
      +---+-------+
      |age|   name|
      +---+-------+
      | 16|Michael|
      | 29| Justin|
      | 46|Michael|
      +---+-------+
      */

    // select算子解析
    personsDS.select("name").show()

    /**
      +-------+
      |   name|
      +-------+
      |Michael|
      |   Andy|
      | Justin|
      | Justin|
      |Michael|
      +-------+
      */

    // groupBy算子解析
    personsDS.groupBy($"name").count().show()

    /**
      +-------+-----+
      |   name|count|
      +-------+-----+
      |Michael|    2|
      |   Andy|    1|
      | Justin|    2|
      +-------+-----+
      */

    // agg算子解析，使用agg算子concat内置函数，将姓名、年龄连接在一起，成为单个字符串列
    personsDS.groupBy($"name", $"age").agg(concat($"name", $"age")).show()

    /**
      +-------+---+-----------------+
      |   name|age|concat(name, age)|
      +-------+---+-----------------+
      | Justin| 29|         Justin29|
      |   Andy| 30|           Andy30|
      |Michael| 16|        Michael16|
      |Michael| 46|        Michael46|
      | Justin| 19|         Justin19|
      +-------+---+-----------------+
      */
    // col算子解析 (和$"name" === $"n"效果一样)
    personsDS.joinWith(personsScoresDS, personsDS.col("name") === personsScoresDS.col("n")).show()

    /**
      +-------------+-------------+
      |           _1|           _2|
      +-------------+-------------+
      |[16, Michael]|[Michael, 88]|
      |   [30, Andy]|  [Andy, 100]|
      | [19, Justin]| [Justin, 89]|
      | [29, Justin]| [Justin, 89]|
      |[46, Michael]|[Michael, 88]|
      +-------------+-------------+
      */

    // collect_list、 collect_set函数解析
    personsDS.groupBy($"name")
      .agg(collect_list($"name"), collect_set($"name"))
      .show()

    /**
      +-------+------------------+-----------------+
      |   name|collect_list(name)|collect_set(name)|
      +-------+------------------+-----------------+
      |Michael|[Michael, Michael]|        [Michael]|
      |   Andy|            [Andy]|           [Andy]|
      | Justin|  [Justin, Justin]|         [Justin]|
      +-------+------------------+-----------------+
      */

    // sum、avg、 max、 min、 count、 countDistinct、 mean、 current_date 函数解析
    personsDS.groupBy($"name")
      .agg(sum($"age"), avg($"age"), max($"age"), min($"age"), count($"age"), countDistinct($"age"), mean($"age"), current_date())
      .show()

    /**
      +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
      |   name|sum(age)|avg(age)|max(age)|min(age)|count(age)|count(DISTINCT age)|avg(age)|current_date()|
      +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
      |Michael|      62|    31.0|      46|      16|         2|                  2|    31.0|    2019-08-20|
      |   Andy|      30|    30.0|      30|      30|         1|                  1|    30.0|    2019-08-20|
      | Justin|      48|    24.0|      29|      19|         2|                  2|    24.0|    2019-08-20|
      +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
      */

    // 这章说实话有点水，坚持敲完当提升熟练度了
    spark.stop()

  }
}
