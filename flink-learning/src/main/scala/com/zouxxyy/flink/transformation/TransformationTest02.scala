package com.zouxxyy.flink.transformation

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
 * Flink转换测试 （ k, v 类型）
 */

object TransformationTest02 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val wordsKV: DataStream[(String, Long)] = env.readTextFile("data/input/test01.txt")
      .flatMap(item => item.split(" "))
        .map(item => (item, 1L))

    /**
     * 1. keyBy
     * 聚合得到 KeyedStream，可指定聚合字段
     */
    val streamKeyBy: KeyedStream[(String, Long), Tuple] = wordsKV.keyBy(0)
    streamKeyBy.print()


    /**
     * 2. reduce
     * 不引入window，reduce 会出现许多中间结果
     */
    streamKeyBy.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    ).print()


    env.execute("TransformationJob")
  }
}
