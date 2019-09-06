package com.zouxxyy.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

/**
 * UDF：用户自定义函数
 */

object UDF {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UDF")

    // 创建SparkSession
    val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    import spark.implicits._

    // 从json中read得到的是DataFrame
    val frame: DataFrame = spark.read.json("data/input/user/user.json")

    frame.createOrReplaceTempView("user")

    // 案例一：自定义一个简单的函数测试
    spark.udf.register("addName", (x:String)=> "Name:"+x)

    spark.sql("select addName(name) from user").show()

    // 案例二：自定义一个弱类型聚合函数测试

    val udaf1 = new MyAgeAvgFunction

    spark.udf.register("avgAge", udaf1)

    spark.sql("select avgAge(age) from user").show()

    // 案例三：自定义一个强类型聚合函数测试

    val udaf2 = new MyAgeAvgClassFunction

    // 将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf2.toColumn.name("aveAge")

    // 用强类型的Dataset的DSL风格的编程语法
    val userDS: Dataset[UserBean] = frame.as[UserBean]

    userDS.select(avgCol).show()

    spark.stop()
  }
}

/**
 * 自定义内聚函数(弱类型)
 */

class MyAgeAvgFunction extends UserDefinedAggregateFunction{

  // 输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  // 计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 函数返回的数据类型
  override def dataType: DataType = DoubleType

  // 函数是否稳定
  override def deterministic: Boolean = true

  // 计算前缓存区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 没有名称，只有结构
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 根据查询结果，更新缓存区的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 多个节点的缓存区的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算缓存区里的东西，得最终返回结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}


/**
 * 自定义内聚函数(强类型)
 */

case class UserBean (name : String, age : BigInt) // 文件读取数字默认是BigInt
case class AvgBuffer(var sum: BigInt, var count: Int)

class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {

  // 初始化缓存区
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  // 输入数据和缓存区计算
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    // 返回b
    b
  }

  // 缓存区的合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count

    b1
  }

  // 计算返回值
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}