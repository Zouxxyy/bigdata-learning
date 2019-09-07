package com.zouxxyy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * SparkStreaming 初探
 */

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    // 每5秒采集一次数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 方式1：从指定端口采集数据
    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

    // 方式2：从指定文件夹中获取
    val fileDStream: DStream[String] = streamingContext.textFileStream("data/input/test")

    // 将采集的数据进行分解(扁平化)
//    val wordDStream: DStream[String] = socketLineDStream.flatMap(line => line.split(" "))
    val wordDStream: DStream[String] = fileDStream.flatMap(line => line.split(" "))

    // 转变数据的结构
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    wordToSumDStream.print()

    // 不能停止采集
    // streamingContext.stop()

    // 启动采集器
    streamingContext.start()

    // Driver等待采集器的执行
    streamingContext.awaitTermination()

  }
}
