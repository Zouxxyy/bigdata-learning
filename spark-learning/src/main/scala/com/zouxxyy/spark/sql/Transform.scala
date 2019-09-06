package com.zouxxyy.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * RDD、DataFrame、DataSet 三者的相互转换
 * 记住RDD到DataFrame是加结构信息；再到DataSet是加类属性，也就是让RDD里存的是一个个对象
 */

object Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transform")

    // 创建SparkSession
    val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    // 创建RDD，SparkSession中内置了sparkContext
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zxy", 22), (2, "Tom", 23), (3, "Jack", 20)))

    rdd.foreach(data => {
      println(data._1, data._2, data._3)
    })

    /****************RDD -> DataFrame***********************/

    // 引入隐式转换操作
    import spark.implicits._

    // 当前rdd没结构，所以toDF()需要传参数
    val df: DataFrame = rdd.toDF("id", "name", "age")

    /****************DataFrame -> DataSet*********************/

    // 先要有样例类
    val ds: Dataset[User] = df.as[User]

    /****************DataSet -> DataFrame*********************/

    val df1: DataFrame = ds.toDF()

    /****************DataFrame -> RDD*************************/

    // 注意这里rdd1的类型不再是(Int, String, Int)，而是Row
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row => {

      // 获取数据时可以通过索引访问数据，索引从0开始
      println(row.getInt(0), row.getString(1), row.getInt(2))
    })

    /****************RDD -> DataSet***************************/

    // 就是让RDD里存的是对象，再调用toDS()
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()

    /****************DataSet -> RDD***************************/

    // 注意这里rdd1的类型是User
    val rdd2: RDD[User] = userDS.rdd

    // 获取数据可以用对象的属性
    rdd2.foreach(usr => {
      println(usr.id, usr.name, usr.age)
    })

    spark.stop()
  }
}

//样例类
case class User(id:Int, name:String, age:Int)
