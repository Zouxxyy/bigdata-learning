package com.zouxxyy.flink.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Flink window操作测试
 */

object WindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 基于socket的数据源 (nc -l 11111)
    val stream = env.socketTextStream("localhost", 11111)

    val streamKeyBy: KeyedStream[(String, Long), Tuple] = stream.map(item => (item, 1L)).keyBy(0)

    /**
     * countWindow 滚动
     * 达到数量时，就计算（单个key）
     */
    val tumblingCountWindow = streamKeyBy.countWindow(5)

    /**
     * countWindow 滑动
     * 步长达到，就执行
     */
    val slideCountWindow = streamKeyBy.countWindow(5, 2)

    /**
     * timeWindow 滚动
     * 达到时间，就计算
     */
    val tumblingTimeWindow = streamKeyBy.timeWindow(Time.seconds(5))

    /**
     * timeWindow 滑动
     */
    val slideTimeWindow = streamKeyBy.timeWindow(Time.seconds(10), Time.seconds(2))

    // 可以发现滑动窗口步长等于窗口大小时就是滚动窗口

    slideTimeWindow.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    ).print()


    env.execute("WindowJob")
  }
}
