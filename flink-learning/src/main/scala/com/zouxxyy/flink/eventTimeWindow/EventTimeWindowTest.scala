package com.zouxxyy.flink.eventTimeWindow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Flink EventTimeWindow操作测试
 */

object EventTimeWindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 基于socket的数据源 (nc -l 11111)
    val stream = env.socketTextStream("localhost", 11111).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(3000)) { // 延迟3秒
        override def extractTimestamp(t: String): Long = {
          // 数据格式: eventTime word
          val eventTime = t.split(" ")(0).toLong
          println(eventTime)
          eventTime
        }
      }
    ).map(item => (item.split(" ")(1), 1L)).keyBy(0)

    /**
     * EventTimeWindow 滚动
     * 其实就是把时间换成EventTime，多了个延迟计算时间设置，其实和timeWindow很像
     */
    val tumblingEventTimeWindow = stream.window(TumblingEventTimeWindows.of(Time.seconds(5)))


    /**
     * EventTimeWindow 滑动
     */
    val slidingEventTimeWindow = stream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))


    /**
     * EventTimeWindow session
     * 相邻数据差5s就计算一次
     */
    val eventTimeSessionWindow = stream.window(EventTimeSessionWindows.withGap(Time.seconds(5)))


    eventTimeSessionWindow.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    ).print()


    env.execute("EventTimeWindowJob")
  }
}
