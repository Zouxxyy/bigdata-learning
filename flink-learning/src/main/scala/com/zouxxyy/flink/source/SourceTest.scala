package com.zouxxyy.flink.source

import org.apache.flink.streaming.api.scala._

/**
 * Flink数据源测试
 */

object SourceTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 基于TextFile的数据源
    // val stream = env.readTextFile("data/input/test00.txt")

    // 基于socket的数据源 (nc -l 11111)
    // val stream = env.socketTextStream("localhost", 11111)

    // 基于集合的数据源
    val list = List(1, 2, 3, 4)
    // val stream = env.fromCollection(list)

    val stream = env.generateSequence(1, 10)

    stream.print()

    env.execute("SourceJob")

  }

}
