package com.zouxxyy.flink.transformation

import org.apache.flink.streaming.api.scala._

/**
 * Flink转换测试 （value类型）
 */

object TransformationTest01 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val list = env.generateSequence(1, 10)
    val test00 = env.readTextFile("data/input/test00.txt")
    val test01 = env.readTextFile("data/input/test01.txt")
    val words00 = test00.flatMap(item => item.split(" "))
    val words01 = test01.flatMap(item => item.split(" "))


    /**
     * 1. map
     */
    // list.map(item => item * 2).print()


    /**
     * 2. flatMap
     */
    // test00.flatMap(item => item.split(" ")).print()


    /**
     * 3. filter
     */
    // list.filter(item => item % 2 == 0).print()


    /**
     * 4. connect  + coMap
     * 2个流合并后得到 ConnectedStreams，再执行map得到 DataStream
     */
    val streamConnect: ConnectedStreams[Long, String] = list.connect(words00)
    val streamComap: DataStream[Any] = streamConnect.map(item => item * 2, item => (item, 1L))
    // streamComap.print()


    /**
     * 5. split + select
     * split分流后，可以配合select使用（以后会删除）
     */
    val streamSplit: SplitStream[String] = words00.split(
      word =>
        ("Flink".equals(word)) match {
          case true => List("Flink")
          case false => List("other")
        }
    )
    // streamSplit.select("Flink").print()
    // streamSplit.select("other").print()


    /**
     * 6. union
     * 合并后还是 DataStream
     */
    val unionStream: DataStream[String] = words00.union(words01)
    // unionStream.print()

    env.execute("TransformationJob")
  }
}
