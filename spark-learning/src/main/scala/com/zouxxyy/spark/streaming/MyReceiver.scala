package com.zouxxyy.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

/**
 * 自定义采集器测试
 */

object MyReceiver {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyReceiver")

    // 每5秒采集一次数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val receiverDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new ZxyReceiver("localhost", 9999))

    val wordDStream: DStream[String] = receiverDStream.flatMap(line => line.split(" "))

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

// 声明采集器

class ZxyReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  var socket : Socket = null

  def receive(): Unit = {
    socket = new Socket(host, port)
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
    var line:String = null
    while ((line = reader.readLine()) != null) {
      // 将采集的数据存储到采集器的内部
      if ("END".equals(line)) {
        return
      }
      else {
        this.store(line)
      }
    }
  }

  override def onStart(): Unit = {

    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()
  }

  override def onStop(): Unit = {

    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}
