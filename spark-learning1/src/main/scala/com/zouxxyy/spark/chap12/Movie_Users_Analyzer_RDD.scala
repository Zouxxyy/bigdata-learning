package com.zouxxyy.spark.chap12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  * 第12章：大数据电影点评系统应用案例
  */

object Movie_Users_Analyzer_RDD {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) // 设置日志打印级别

    var masterUrl = "local[*]"
    var dataPath = "data/chap01/moviedata/medium/"

    val sc = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName("Movie_Users_Analyzer_RDD"))

    // 读取RDD
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    /**
      * 1. 电影点评系统用户行为分析：分析具体某部电影观看的用户信息，例如电影ID为1193的
      *   用户信息（用户的ID、Age、Gender、Occupation）
      */

    val usersBasic = usersRDD.map(_.split("::"))
      .map(user => (user(3), (user(0), user(1), user(2)))) // OccupationID,(UserID, Gender, Age)

    // 还有1岁的？
    for (elem <- usersBasic.collect().take(2)) {
      println("userBasicRDD: " + elem)
    }

    val occupations = occupationsRDD.map(_.split("::")).map(job => (job(0), job(1))) // OccupationID, Occupation

    val userInformation = usersBasic.join(occupations)

    userInformation.cache()

    for (elem <- userInformation.collect().take(2)) {
      println("userInformation: " + elem)
    }

    /**
      userInformation: (4,((25,M,18),college/grad student))
      userInformation: (4,((38,F,18),college/grad student))
      */

    val targetMovie = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1)))
      .filter(_._2.equals("1193"))

    for (elem <- targetMovie.collect().take(2)) {
      println("targetMovie: " + elem)
    }

    /**
      targetMovie: (1,1193)
      targetMovie: (2,1193)
      */

    val targetUsers = userInformation.map(x => (x._2._1._1, x._2)) // (UserID, (...))

    val userInformationForSpecificMovie = targetMovie.join(targetUsers)
    for (elem <- userInformationForSpecificMovie.collect().take(10)) {
      println("userInformationForSpecificMovie: " + elem)
    }

    /**
      userInformationForSpecificMovie: (3492,(1193,((3492,M,35),executive/managerial)))
      userInformationForSpecificMovie: (4286,(1193,((4286,M,35),technician/engineer)))
      userInformationForSpecificMovie: (5317,(1193,((5317,F,25),other or not specified)))
      userInformationForSpecificMovie: (5975,(1193,((5975,M,25),sales/marketing)))
      userInformationForSpecificMovie: (792,(1193,((792,M,25),technician/engineer)))
      userInformationForSpecificMovie: (4088,(1193,((4088,M,25),clerical/admin)))
      userInformationForSpecificMovie: (5430,(1193,((5430,F,45),academic/educator)))
      userInformationForSpecificMovie: (5111,(1193,((5111,M,35),artist)))
      userInformationForSpecificMovie: (178,(1193,((178,M,56),technician/engineer)))
      userInformationForSpecificMovie: (749,(1193,((749,M,35),tradesman/craftsman)))
      */


    /**
      * 2. 电影流行度分析：所有电影中平均得分最高（口碑最好）的电影(第一节已做)及观看人数最高的电影（流行度最高）
      */

    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2))).cache()

    println ("所有电影中粉丝或者观看人数最多的电影:")
    ratings.map(x => (x._2, 1)) // (MovieID, 1)
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1)).take(10).foreach(println)

    /**
      所有电影中粉丝或者观看人数最多的电影:
      (2858,3428)
      (260,2991)
      (1196,2990)
      (1210,2883)
      (480,2672)
      (2028,2653)
      (589,2649)
      (2571,2590)
      (1270,2583)
      (593,2578)
      */


    /**
      * 3. 最受不同年龄段人员欢迎的电影TopN
      * "users.dat"：UserID::Gender::Age::OccupationID::Zip-code
      * 思路：用空间换时间
      * 先构建年龄为18的UserID的HashSet，再根据 HashSet 过滤 Rating
      */


    //
    val targetQQUsers = usersRDD.map(_.split("::")).map(x => (x(0), x(2))) // (UserID, Age)
      .filter(_._2.equals("18"))

    val targetTaobaoUsers = usersRDD.map(_.split("::")).map(x => (x(0), x(2))) // (UserID, Age)
      .filter(_._2.equals("25"))

    // ++ 将 collect()返回的Array[T] 变成 HashSet[String]
    val targetQQUsersSet = HashSet() ++ targetQQUsers.map(_._1).collect()
    val targetTaobaoUsersSet = HashSet() ++ targetTaobaoUsers.map(_._1).collect()

    val targetQQUsersBroadcast = sc.broadcast(targetQQUsersSet)
    val targetTaobaoUsersBroadcast = sc.broadcast(targetTaobaoUsersSet)


    val movieID2Name = moviesRDD.map(_.split("::")).map(x => (x(0), x(1))).collect.toMap

    println("所有电影中QQ或者微信核心目标用户最喜爱电影TopN分析:")
    ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))) // (UserID, MovieID)
      .filter(x => targetQQUsersBroadcast.value.contains(x._1))
      .map(x => (x._2, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)) // (MovieID, count)
      .take(10)
      .map(x => (movieID2Name.getOrElse(x._1, null), x._2)).foreach(println)

    println("所有电影中淘宝核心目标用户最喜爱电影TopN分析:")
    ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))) // (UserID, MovieID)
      .filter(x => targetTaobaoUsersBroadcast.value.contains(x._1))
      .map(x => (x._2, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)) // (MovieID, count)
      .take(10)
      .map(x => (movieID2Name.getOrElse(x._1, null), x._2)).foreach(println)


    /**
      所有电影中QQ或者微信核心目标用户最喜爱电影TopN分析:
      (American Beauty (1999),715)
      (Star Wars: Episode VI - Return of the Jedi (1983),586)
      (Star Wars: Episode V - The Empire Strikes Back (1980),579)
      (Matrix, The (1999),567)
      (Star Wars: Episode IV - A New Hope (1977),562)
      (Braveheart (1995),544)
      (Saving Private Ryan (1998),543)
      (Jurassic Park (1993),541)
      (Terminator 2: Judgment Day (1991),529)
      (Men in Black (1997),514)
      所有电影中淘宝核心目标用户最喜爱电影TopN分析:
      (American Beauty (1999),1334)
      (Star Wars: Episode V - The Empire Strikes Back (1980),1176)
      (Star Wars: Episode VI - Return of the Jedi (1983),1134)
      (Star Wars: Episode IV - A New Hope (1977),1128)
      (Terminator 2: Judgment Day (1991),1087)
      (Silence of the Lambs, The (1991),1067)
      (Matrix, The (1999),1049)
      (Saving Private Ryan (1998),1017)
      (Back to the Future (1985),1001)
      (Jurassic Park (1993),1000)
     */


    sc.stop()

  }
}
