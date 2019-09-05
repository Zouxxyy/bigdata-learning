package com.zouxxyy.spark.chap01

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1.1节 通过 RDD 实战电影点评系统入门
  */

object MovieAnalyzer1 {
  def main(args: Array[String]): Unit = {

    val dataPath = "data/chap01/moviedata/medium/" //数据存放的目录

    // 指定本地运行与程序名字
    val conf = new SparkConf().setMaster("local[*]").setAppName ("MovieAnalyzer1")
    val spark= SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")


    /**
      * 案例一：打印出所有电影中评分最高的前10个电影名和平均评分
      */

    println("所有电影中平均得分最高(口碑最好)的电影:")

    val movieInfo = moviesRDD.map(_.split("::")).map(x => (x(0), x(1))).cache()

    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2))).cache() //格式化出电影ID和评分

    val avgRatings = ratings.map(x => (x._2, (x._3.toDouble, 1))) // 取 key: movieID 和 value : (Rating, 1)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // key: movieID , value : (总评分, 人数)
      .map(x => (x._1, x._2._1.toDouble / x._2._2)) // 平均值，Key和Value的交换 用于排序

    avgRatings.join(movieInfo) // (movieID, (avgRatings, movieName))
      .map(item => (item._2._1, item._2._2)) // (avgRatings, movieName)
      .sortByKey(false) // 降序
      .take(10) // 取前10
      .foreach(record => println(record._2 + " 评分为: " + record._1))


    /**
      * 案例二：分析最受男性喜爱的电影 Top10 和最受女性喜爱的电影 Top10
      */

    val usersGender = usersRDD.map(_.split("::")).map(x => (x(0), x(1)))

    // 第一步：用join连接rating表和gender表
    val genderRatings = ratings.map(x => (x._1, (x._1, x._2, x._3)))
      .join(usersGender).cache() // (userID, ((userID, movieID, rating), gender))

    // 第二步：得到男性评分表，女性评分表
    val maleFilteredRatings = genderRatings
      .filter(x => x._2._2.equals("M")).map(x => x._2._1) // (userID, movieID, rating)
    val femaleFilteredRatings = genderRatings
      .filter(x => x._2._2.equals("F")).map(x => x._2._1)

    // 第三步：使用案例一的方法，分别打印前10
    println("\n所有电影中最受男性喜爱的电影Top1O:")

    maleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1))) // 取 key: movieID 和 value : (Rating, 1)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // key: movieID , value : (总评分, 人数)
      .map(x => (x._1, x._2._1.toDouble / x._2._2)) // 平均值，Key和Value的交换 用于排序
      .join(movieInfo) // (movieID, (avgRatings, movieName))
      .map(item => (item._2._1, item._2._2)) // (avgRatings, movieName)
      .sortByKey(false) // 降序
      .take(10) // 取前10
      .foreach(record => println(record._2 + " 评分为: " + record._1))

    println("\n所有电影中最受女性喜爱的电影Top1O:")

    femaleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1))) // 取 key: movieID 和 value : (Rating, 1)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // key: movieID , value : (总评分, 人数)
      .map(x => (x._1, x._2._1.toDouble / x._2._2)) // 平均值，Key和Value的交换 用于排序
      .join(movieInfo) // (movieID, (avgRatings, movieName))
      .map(item => (item._2._1, item._2._2)) // (avgRatings, movieName)
      .sortByKey(false) // 降序
      .take(10) // 取前10
      .foreach(record => println(record._2 + " 评分为: " + record._1))


    /**
      * 案例三：对电影评分数据进行二次排序，以 Timestamp 和 Rating 两个维度降序排列
      */

    println("\n对电影评分数据以 Timestamp 和 Rating 两个维度进行二次降序排列 :")

    val pairWithSortKey = ratingsRDD
      .map(line => {
        val splited = line.split("::")
        (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line) // (2此排序key, line)
      })

    // 直接用sortByKey排序
    val sorted = pairWithSortKey.sortByKey(false)

    // 打印
    sorted.map(line => line._2)
      .take(10)
      .foreach(println)


    spark.stop()

  }
}

class SecondarySortKey(val first: Double, val second: Double)
  extends Ordered[SecondarySortKey] with Serializable {
  // 重写compare方法
  override def compare(other: SecondarySortKey): Int = {
    if (this.first - other.first != 0) {
      (this.first - other.first).toInt
    } else {
      if (this.second - other.second > 0) {
        Math.ceil(this.second - other.second).toInt
      } else if (this.second - other.second < 0) {
        Math.floor(this.second - other.second).toInt
      } else {
        (this.second - other.second).toInt
      }
    }
  }
}



/*
所有电影中平均得分最高(口碑最好)的电影:
Schlafes Bruder (Brother of Sleep) (1995) 评分为: 5.0
Gate of Heavenly Peace, The (1995) 评分为: 5.0
Lured (1947) 评分为: 5.0
Bittersweet Motel (2000) 评分为: 5.0
Follow the Bitch (1998) 评分为: 5.0
Song of Freedom (1936) 评分为: 5.0
One Little Indian (1973) 评分为: 5.0
Baby, The (1973) 评分为: 5.0
Smashing Time (1967) 评分为: 5.0
Ulysses (Ulisse) (1954) 评分为: 5.0

所有电影中最受男性喜爱的电影Top1O:
Schlafes Bruder (Brother of Sleep) (1995) 评分为: 5.0
Gate of Heavenly Peace, The (1995) 评分为: 5.0
Lured (1947) 评分为: 5.0
Bells, The (1926) 评分为: 5.0
Dangerous Game (1993) 评分为: 5.0
Small Wonders (1996) 评分为: 5.0
Follow the Bitch (1998) 评分为: 5.0
Angela (1995) 评分为: 5.0
Baby, The (1973) 评分为: 5.0
Smashing Time (1967) 评分为: 5.0

所有电影中最受女性喜爱的电影Top1O:
Big Combo, The (1955) 评分为: 5.0
Gate of Heavenly Peace, The (1995) 评分为: 5.0
Ayn Rand: A Sense of Life (1997) 评分为: 5.0
Raw Deal (1948) 评分为: 5.0
Woman of Paris, A (1923) 评分为: 5.0
Bittersweet Motel (2000) 评分为: 5.0
I Am Cuba (Soy Cuba/Ya Kuba) (1964) 评分为: 5.0
Coldblooded (1995) 评分为: 5.0
Twice Upon a Yesterday (1998) 评分为: 5.0
Song of Freedom (1936) 评分为: 5.0

对电影评分数据以 Timestamp 和 Rating 两个维度进行二次降序排列 :
4958::1924::4::1046454590
4958::3264::4::1046454548
4958::2634::3::1046454548
4958::1407::5::1046454443
4958::2399::1::1046454338
4958::3489::4::1046454320
4958::2043::1::1046454282
4958::2453::4::1046454260
5312::3267::4::1046444711
5948::3098::4::1046437932
 */