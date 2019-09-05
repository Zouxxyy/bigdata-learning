package com.zouxxyy.spark.chap15


import scala.language.postfixOps
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.{Map, mutable}

/**
  * 第15章：NBA篮球运动员大数据分析系统应用案例
  */
object NBABasketball_Analysis {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[*]"

    val conf = new SparkConf().setMaster(masterUrl).set("spark.sql.shuffle.partitions", "5").setAppName("FantasyBasketball")

    val spark = SparkSession
      .builder()
      .appName("NBABasketball_Analysis")
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val DATA_PATH = "data/chap15/NBABasketball/"
    val TMP_PATH = "data/chap15/basketball_tmp/"

    // 生成数据
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(TMP_PATH), true)

    //process files so that each line includes the year
    for (i <- 1970 to 2016) {
      //println(i)
      val yearStats = sc.textFile(s"${DATA_PATH}/leagues_NBA_$i*").repartition(sc.defaultParallelism)
      yearStats.filter(x => x.contains(",")).map(x => (i, x)).saveAsTextFile(s"${TMP_PATH}/NBAStatsPerYear/$i/")
    }


    /******************************工具：类、辅助函数、变量**************************/

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    import org.apache.spark.util.StatCounter

    import scala.collection.mutable.ListBuffer

    // 工具1：计算归一化的辅助函数
    def statNormalize(stat: Double, max: Double, min: Double) = {
      val newmax = math.max(math.abs(max), math.abs(min))
      stat / newmax
    }

    // 工具2：初始化+权重统计+归一统计
    case class BballData(val year: Int, name: String, position: String,
                         age: Int, team: String, gp: Int, gs: Int, mp: Double,
                         stats: Array[Double], statsZ: Array[Double] = Array[Double](),
                         valueZ: Double = 0, statsN: Array[Double] = Array[Double](),
                         valueN: Double = 0, experience: Double = 0)

    // 工具3：解析转换为 BBallDataZ 对象
    def bbParse(input: String, bStats: scala.collection.Map[String, Double] = Map.empty,
                zStats: scala.collection.Map[String, Double] = Map.empty): BballData = {
      val line = input.replace(",,", ",0,")
      val pieces = line.substring(1, line.length - 1).split(",")
      val year = pieces(0).toInt
      val name = pieces(2)
      val position = pieces(3)
      val age = pieces(4).toInt
      val team = pieces(5)
      val gp = pieces(6).toInt
      val gs = pieces(7).toInt
      val mp = pieces(8).toDouble

      val stats: Array[Double] = pieces.slice(9, 31).map(x => x.toDouble)
      var statsZ: Array[Double] = Array.empty
      var valueZ: Double = Double.NaN
      var statsN: Array[Double] = Array.empty
      var valueN: Double = Double.NaN

      if (!bStats.isEmpty) {
        val fg: Double = (stats(2) - bStats.apply(year.toString + "_FG%_avg")) * stats(1)
        val tp = (stats(3) - bStats.apply(year.toString + "_3P_avg")) / bStats.apply(year.toString + "_3P_stdev")
        val ft = (stats(12) - bStats.apply(year.toString + "_FT%_avg")) * stats(11)
        val trb = (stats(15) - bStats.apply(year.toString + "_TRB_avg")) / bStats.apply(year.toString + "_TRB_stdev")
        val ast = (stats(16) - bStats.apply(year.toString + "_AST_avg")) / bStats.apply(year.toString + "_AST_stdev")
        val stl = (stats(17) - bStats.apply(year.toString + "_STL_avg")) / bStats.apply(year.toString + "_STL_stdev")
        val blk = (stats(18) - bStats.apply(year.toString + "_BLK_avg")) / bStats.apply(year.toString + "_BLK_stdev")
        val tov = (stats(19) - bStats.apply(year.toString + "_TOV_avg")) / bStats.apply(year.toString + "_TOV_stdev") * (-1)
        val pts = (stats(21) - bStats.apply(year.toString + "_PTS_avg")) / bStats.apply(year.toString + "_PTS_stdev")
        statsZ = Array(fg, ft, tp, trb, ast, stl, blk, tov, pts)
        valueZ = statsZ.reduce(_ + _)

        if (!zStats.isEmpty) {
          val zfg = (fg - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev")
          val zft = (ft - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev")
          val fgN = statNormalize(zfg, (zStats.apply(year.toString + "_FG_max") - zStats.apply(year.toString + "_FG_avg"))
            / zStats.apply(year.toString + "_FG_stdev"), (zStats.apply(year.toString + "_FG_min")
            - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev"))
          val ftN = statNormalize(zft, (zStats.apply(year.toString + "_FT_max") - zStats.apply(year.toString + "_FT_avg"))
            / zStats.apply(year.toString + "_FT_stdev"), (zStats.apply(year.toString + "_FT_min")
            - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev"))
          val tpN = statNormalize(tp, zStats.apply(year.toString + "_3P_max"), zStats.apply(year.toString + "_3P_min"))
          val trbN = statNormalize(trb, zStats.apply(year.toString + "_TRB_max"), zStats.apply(year.toString + "_TRB_min"))
          val astN = statNormalize(ast, zStats.apply(year.toString + "_AST_max"), zStats.apply(year.toString + "_AST_min"))
          val stlN = statNormalize(stl, zStats.apply(year.toString + "_STL_max"), zStats.apply(year.toString + "_STL_min"))
          val blkN = statNormalize(blk, zStats.apply(year.toString + "_BLK_max"), zStats.apply(year.toString + "_BLK_min"))
          val tovN = statNormalize(tov, zStats.apply(year.toString + "_TOV_max"), zStats.apply(year.toString + "_TOV_min"))
          val ptsN = statNormalize(pts, zStats.apply(year.toString + "_PTS_max"), zStats.apply(year.toString + "_PTS_min"))
          statsZ = Array(zfg, zft, tp, trb, ast, stl, blk, tov, pts)
          //  //println("bbParse函数中打印statsZ： " + statsZ.foreach(//println(_)) )
          valueZ = statsZ.reduce(_ + _)
          statsN = Array(fgN, ftN, tpN, trbN, astN, stlN, blkN, tovN, ptsN)
          //   //println("bbParse函数中打印statsN： " + statsN.foreach(//println(_)) )
          valueN = statsN.reduce(_ + _)
        }
      }
      BballData(year, name, position, age, team, gp, gs, mp, stats, statsZ, valueZ, statsN, valueN)
    }

    // 工具4：该类是一个辅助工具类，在后面编写业务代码的时候会反复使用其中的方法
    class BballStatCounter extends Serializable {
      val stats: StatCounter = new StatCounter()
      var missing: Long = 0

      def add(x: Double): BballStatCounter = {
        if (x.isNaN) {
          missing += 1
        } else {
          stats.merge(x)
        }
        this
      }

      def merge(other: BballStatCounter): BballStatCounter = {
        stats.merge(other.stats)
        missing += other.missing
        this
      }

      def printStats(delim: String): String = {
        stats.count + delim + stats.mean + delim + stats.stdev + delim + stats.max + delim + stats.min
      }

      override def toString: String = {
        "stats: " + stats.toString + " NaN: " + missing
      }
    }

    object BballStatCounter extends Serializable {
      def apply(x: Double) = new BballStatCounter().add(x) //在这里使用了Scala语言的一个编程技巧，借助于apply工厂方法，在构造该对象的时候就可以执行出结果
    }

    // 工具5：处理原始数据为 zScores and nScores
    def processStats(stats0: org.apache.spark.rdd.RDD[String], txtStat: Array[String],
                     bStats: scala.collection.Map[String, Double] = Map.empty,
                     zStats: scala.collection.Map[String, Double] = Map.empty): RDD[(String, Double)] = {
      // 解析stats
      val stats1: RDD[BballData] = stats0.map(x => bbParse(x, bStats, zStats))

      // 按年份进行分组
      val stats2: RDD[(Int, Iterable[Array[Double]])] = {
        if (bStats.isEmpty) {
          stats1.keyBy(x => x.year).map(x => (x._1, x._2.stats)).groupByKey()
        } else {
          stats1.keyBy(x => x.year).map(x => (x._1, x._2.statsZ)).groupByKey()
        }
      }

      // map each stat to StatCounter
      val stats3: RDD[(Int, Iterable[Array[BballStatCounter]])] = stats2.map { case (x, y) => (x, y.map(a => a.map((b: Double) => BballStatCounter(b)))) }

      // merge all stats together
      val stats4: RDD[(Int, Array[BballStatCounter])] = stats3.map { case (x, y) => (x, y.reduce((a, b) => a.zip(b).map { case (c, d) => c.merge(d) })) }

      // combine stats with label and pull label out
      val stats5: RDD[Array[(Int, String, BballStatCounter)]] = stats4.map { case (x, y: Array[BballStatCounter]) => (x, txtStat.zip(y)) }.map {
        x =>
          (x._2.map {
            case (y, z) => (x._1, y, z)
          })
      }

      // separate each stat onto its own line and print out the Stats to a String
      val stats6: RDD[(Int, String, String)] = stats5.flatMap(x => x.map(y => (y._1, y._2, y._3.printStats(","))))
      //
      //println todo("测试一下转换========")
      // stats6.take(1).foreach(println)
      //turn stat tuple into key-value pairs with corresponding agg stat
      val stats7: RDD[(String, Double)] = stats6.flatMap { case (a, b, c) => {
        val pieces = c.split(",")
        val count = pieces(0)
        val mean = pieces(1)
        val stdev = pieces(2)
        val max = pieces(3)
        val min = pieces(4)

        Array((a + "_" + b + "_" + "count", count.toDouble),
          (a + "_" + b + "_" + "avg", mean.toDouble),
          (a + "_" + b + "_" + "stdev", stdev.toDouble),
          (a + "_" + b + "_" + "max", max.toDouble),
          (a + "_" + b + "_" + "min", min.toDouble))
      }
      }
      stats7
    }

    // 工具6：处理经验值函数
    def processStatsAgeOrExperience(stats0: org.apache.spark.rdd.RDD[(Int, Array[Double])], label: String): DataFrame = {


      //group elements by age
      val stats1: RDD[(Int, Iterable[Array[Double]])] = stats0.groupByKey()

      val stats2: RDD[(Int, Iterable[Array[BballStatCounter]])] = stats1.map {
        case (x: Int, y: Iterable[Array[Double]]) =>
          (x, y.map((z: Array[Double]) => z.map((a: Double) => BballStatCounter(a))))
      }
      //Reduce rows by merging StatCounter objects
      val stats3: RDD[(Int, Array[BballStatCounter])] = stats2.map { case (x, y) => (x, y.reduce((a, b) => a.zip(b).map { case (c, d) => c.merge(d) })) }
      //turn data into RDD[Row] object for dataframe
      val stats4 = stats3.map(x => Array(Array(x._1.toDouble),
        x._2.flatMap(y => y.printStats(",").split(",")).map(y => y.toDouble)).flatMap(y => y))
        .map(x =>
          Row(x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8),
            x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20)))

      //create schema for age table
      val schema = StructType(
        StructField(label, IntegerType, true) ::
          StructField("valueZ_count", DoubleType, true) ::
          StructField("valueZ_mean", DoubleType, true) ::
          StructField("valueZ_stdev", DoubleType, true) ::
          StructField("valueZ_max", DoubleType, true) ::
          StructField("valueZ_min", DoubleType, true) ::
          StructField("valueN_count", DoubleType, true) ::
          StructField("valueN_mean", DoubleType, true) ::
          StructField("valueN_stdev", DoubleType, true) ::
          StructField("valueN_max", DoubleType, true) ::
          StructField("valueN_min", DoubleType, true) ::
          StructField("deltaZ_count", DoubleType, true) ::
          StructField("deltaZ_mean", DoubleType, true) ::
          StructField("deltaZ_stdev", DoubleType, true) ::
          StructField("deltaZ_max", DoubleType, true) ::
          StructField("deltaZ_min", DoubleType, true) ::
          StructField("deltaN_count", DoubleType, true) ::
          StructField("deltaN_mean", DoubleType, true) ::
          StructField("deltaN_stdev", DoubleType, true) ::
          StructField("deltaN_max", DoubleType, true) ::
          StructField("deltaN_min", DoubleType, true) :: Nil
      )

      //create data frame
      spark.createDataFrame(stats4, schema)
    }


    /***********************************开始处理数据**************************************/


    /**
      * 功能0：数据读取与清理
      */

    // 数据读取
    val stats = sc.textFile(s"${TMP_PATH}/NBAStatsPerYear/*/*").repartition(sc.defaultParallelism)

    // 数据清理
    val filteredStats: RDD[String] = stats.filter(x => !x.contains("FG%")).filter(x => x.contains(","))
      .map(x => x.replace("*", "").replace(",,", ",0,"))
    filteredStats.cache()

    println("NBA球员清洗以后的数据记录:  ")
    filteredStats.take(5).foreach(println)

    /**
      (2013,237,Solomon Jones,PF,28,NYK,2,1,13.0,0.0,0.5,.000,0.0,0.0,0,0.0,0.5,.000,.000,0.0,0.0,0,0.5,1.0,1.5,0.0,0.0,0.5,1.5,1.5,0.0)
      (2013,244,Jason Kidd,SG,39,NYK,76,48,26.9,2.0,5.4,.372,1.5,4.3,.351,0.5,1.1,.452,.511,0.5,0.6,.833,0.7,3.6,4.3,3.3,1.6,0.3,1.0,1.6,6.0)
      (2013,248,Brandon Knight,PG,21,DET,75,75,31.5,4.8,11.7,.407,1.6,4.4,.367,3.2,7.3,.430,.475,2.2,3.0,.733,0.7,2.6,3.3,4.0,0.8,0.1,2.7,2.1,13.3)
      (2013,252,Doron Lamb,SG,21,TOT,47,0,12.3,1.3,3.5,.368,0.3,0.8,.378,1.0,2.7,.365,.411,0.4,0.7,.588,0.2,0.8,1.0,0.7,0.3,0.0,0.6,1.1,3.3)
      (2013,256,Courtney Lee,SG,27,BOS,78,39,24.9,3.2,6.8,.464,0.7,2.0,.372,2.4,4.8,.503,.519,0.8,0.9,.861,0.4,2.0,2.4,1.8,1.1,0.3,1.1,1.8,7.8)
      */

    /**
      * 功能1：统计出每年 NBA 球员比赛的各项聚合统计数据。
      *
      * txtStat数组中包含 FG、 FGA、 FG%、 3P、 3PA、3P% 、 2P 、 2PA 、 2P% 、 eFG% 、 FT 、 FTA 、 FT% 、 ORB 、
      * DRB 、 TRB 、 AST 、 STL 、 BLK 、TOV、PF 、PTS 等字符串，其对应为投篮命中次数、投篮出手次数、投篮命中率、
      * 3分命中、 3分出手、 3分命中率、 2分命中、 2分出手、 2 分命中率、有效投篮命中率、罚球命中、罚球出手次数、
      * 罚球命中率、进攻篮板球、防御篮板球、篮板球、助攻、抢断、盖帽、失误、个人犯规、得分 。
      */

    val txtStat: Array[String] = Array("FG", "FGA", "FG%", "3P", "3PA", "3P%", "2P", "2PA", "2P%", "eFG%", "FT",
      "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS")

    val aggStats: Map[String, Double] = processStats(filteredStats, txtStat).collectAsMap //基础数据项，需要在集群中使用，因此会在后面广播出去
    println("NBA球员基础数据项aggStats MAP映射集: ")
    aggStats.take(5).foreach { case (k, v) => println(" （ " + k + "  , " + v + " ) ") }
    // 将 RDD 转换成 map 结构进行广播
    val broadcastStats: Broadcast[Map[String, Double]] = sc.broadcast(aggStats) //使用广播提升效率

    /**
      NBA球员基础数据项aggStats MAP映射集:
     （ 1984_FTA_stdev  , 1.9128851655569135 )
     （ 1990_TRB_avg  , 3.6524934383202097 )
     （ 1996_FTA_stdev  , 1.8053726969894366 )
     （ 2008_FT_count  , 451.0 )
     （ 1988_2P_min  , 0.0 )
      */


    /**
      * 功能2：根据 NBA 球员 1970 年至 2016 年的历史数据，统计出NBA球员数据每年标准分Z-Score
      *
      *  Z-score: 其基本的计算过程是球员的得分减去平均值后除以标准差
      */

    val txtStatZ = Array("FG", "FT", "3P", "TRB", "AST", "STL", "BLK", "TOV", "PTS")
    val zStats: Map[String, Double] = processStats(filteredStats, txtStatZ, broadcastStats.value).collectAsMap
    println("NBA球员Z-Score标准分zStats  MAP映射集: ")
    zStats.take(5).foreach { case (k, v) => println(" （ " + k + "  , " + v + " ) ") }
    // 将 RDD 转换成 map 结构进行广播
    val zBroadcastStats = sc.broadcast(zStats)

    /**
       NBA球员Z-Score标准分zStats  MAP映射集:
     （ 1970_FT_min  , -3.638046198830412 )
     （ 1984_STL_stdev  , 0.9999999999999993 )
     （ 1996_STL_stdev  , 1.0 )
     （ 1994_TRB_min  , -1.3244043555690208 )
     （ 1989_PTS_max  , 3.5366141445725736 )
      */


    /**
      * 功能3：根据 NBA 球员 1970 年至 2016 年的历史数据，统计NBA球员数据每年归一化
      *
      */

    val nStats: RDD[BballData] = filteredStats.map(x => bbParse(x, broadcastStats.value, zBroadcastStats.value))
    println("NBA球员比赛数据归一化处理结果：")
    val nPlayer: RDD[Row] = nStats.map(x => {
      val nPlayerRow: Row = Row.fromSeq(Array(x.name, x.year, x.age, x.position, x.team, x.gp, x.gs, x.mp)
        ++ x.stats ++ x.statsZ ++ Array(x.valueZ) ++ x.statsN ++ Array(x.valueN))
      println( nPlayerRow.mkString(" "))
      nPlayerRow
    })

    // 创建 DataFrame 的元数据结构 schemaN
    val schemaN: StructType = StructType(
      StructField("name", StringType, true) ::
        StructField("year", IntegerType, true) ::
        StructField("age", IntegerType, true) ::
        StructField("position", StringType, true) ::
        StructField("team", StringType, true) ::
        StructField("gp", IntegerType, true) ::
        StructField("gs", IntegerType, true) ::
        StructField("mp", DoubleType, true) ::
        StructField("FG", DoubleType, true) ::
        StructField("FGA", DoubleType, true) ::
        StructField("FGP", DoubleType, true) ::
        StructField("3P", DoubleType, true) ::
        StructField("3PA", DoubleType, true) ::
        StructField("3PP", DoubleType, true) ::
        StructField("2P", DoubleType, true) ::
        StructField("2PA", DoubleType, true) ::
        StructField("2PP", DoubleType, true) ::
        StructField("eFG", DoubleType, true) ::
        StructField("FT", DoubleType, true) ::
        StructField("FTA", DoubleType, true) ::
        StructField("FTP", DoubleType, true) ::
        StructField("ORB", DoubleType, true) ::
        StructField("DRB", DoubleType, true) ::
        StructField("TRB", DoubleType, true) ::
        StructField("AST", DoubleType, true) ::
        StructField("STL", DoubleType, true) ::
        StructField("BLK", DoubleType, true) ::
        StructField("TOV", DoubleType, true) ::
        StructField("PF", DoubleType, true) ::
        StructField("PTS", DoubleType, true) ::
        StructField("zFG", DoubleType, true) ::
        StructField("zFT", DoubleType, true) ::
        StructField("z3P", DoubleType, true) ::
        StructField("zTRB", DoubleType, true) ::
        StructField("zAST", DoubleType, true) ::
        StructField("zSTL", DoubleType, true) ::
        StructField("zBLK", DoubleType, true) ::
        StructField("zTOV", DoubleType, true) ::
        StructField("zPTS", DoubleType, true) ::
        StructField("zTOT", DoubleType, true) ::
        StructField("nFG", DoubleType, true) ::
        StructField("nFT", DoubleType, true) ::
        StructField("n3P", DoubleType, true) ::
        StructField("nTRB", DoubleType, true) ::
        StructField("nAST", DoubleType, true) ::
        StructField("nSTL", DoubleType, true) ::
        StructField("nBLK", DoubleType, true) ::
        StructField("nTOV", DoubleType, true) ::
        StructField("nPTS", DoubleType, true) ::
        StructField("nTOT", DoubleType, true) :: Nil
    )

    // 创建 DaraFrame
    val dfPlayersT: DataFrame = spark.createDataFrame(nPlayer, schemaN)

    // 将所有数据保存为临时表
    dfPlayersT.createOrReplaceTempView("tPlayers")

    // 计算 exp 和 zdiff, ND2FF
    val dfPlayers: DataFrame = spark.sql("select age-min_age as exp,tPlayers.* from tPlayers join" +
      " (select name,min(age)as min_age from tPlayers group by name) as t1" +
      " on tPlayers.name=t1.name order by tPlayers.name, exp  ")
    println("计算exp and zdiff, ndiff")
//    dfPlayers.show()
    // 保存为表
    dfPlayers.createOrReplaceTempView("Players")
    //filteredStats.unpersist()


    /**
      * 功能4：历年比赛数据按球员分组统计分析
      */

    println("打印NBA球员的历年比赛记录：   ")
    dfPlayers.rdd.map(x =>
      (x.getString(1), x)).filter(_._1.contains("A.C. Green"))
    //  .foreach( println)

    val pStats: RDD[(String, Iterable[(Double, Double, Int, Int, Array[Double], Int)])] = dfPlayers.sort(dfPlayers("name"), dfPlayers("exp") asc).rdd.map(x =>
      (x.getString(1), (x.getDouble(50), x.getDouble(40), x.getInt(2), x.getInt(3),
        Array(x.getDouble(31), x.getDouble(32), x.getDouble(33), x.getDouble(34), x.getDouble(35),
          x.getDouble(36), x.getDouble(37), x.getDouble(38), x.getDouble(39)), x.getInt(0))))
      .groupByKey
    pStats.cache

    println("**********根据NBA球员名字分组：   ")
    pStats.take(15).foreach(x => {
      val myx2: Iterable[(Double, Double, Int, Int, Array[Double], Int)] = x._2
      //println("按NBA球员： " + x._1 + " 进行分组，组中元素个数为：" + myx2.size)
      for (i <- 1 to myx2.size) {
        val myx2size: Array[(Double, Double, Int, Int, Array[Double], Int)] = myx2.toArray
        val mynext: (Double, Double, Int, Int, Array[Double], Int) = myx2size(i - 1)
        /* println(i + " : " + x._1 + " , while   " + mynext._1 + " , " + mynext._2 + " , "
            + mynext._3 + " , " + mynext._4 + " ,     " + mynext._5.mkString(" || ") + "     , "
            + mynext._6)*/
      }
    })


    /**
      * 功能5：NBA球员年龄值及经验值列表获取
      */

    import spark.implicits._
    //for each player, go through all the years and calculate the change in valueZ and valueN, save into two lists
    //one for age, one for experience
    //exclude players who played in 1980 from experience, as we only have partial data for them
    val excludeNames: String = dfPlayers.filter(dfPlayers("year") === 1980).select(dfPlayers("name"))
      .map(x => x.mkString).collect().mkString(",")

    val pStats1: RDD[(ListBuffer[(Int, Array[Double])], ListBuffer[(Int, Array[Double])])] = pStats.map { case (name, stats) =>
      var last = 0
      var deltaZ = 0.0
      var deltaN = 0.0
      var valueZ = 0.0
      var valueN = 0.0
      var exp = 0
      val aList = ListBuffer[(Int, Array[Double])]()
      val eList = ListBuffer[(Int, Array[Double])]()
      stats.foreach(z => {
        if (last > 0) {
          deltaN = z._1 - valueN
          deltaZ = z._2 - valueZ
        } else {
          deltaN = Double.NaN
          deltaZ = Double.NaN
        }
        valueN = z._1
        valueZ = z._2
        last = z._4
        aList += ((last, Array(valueZ, valueN, deltaZ, deltaN)))
        if (!excludeNames.contains(z._1)) {
          exp = z._6
          eList += ((exp, Array(valueZ, valueN, deltaZ, deltaN)))
        }
      })
      (aList, eList)
    }

    pStats1.cache

    println("按NBA球员的年龄及经验值进行统计：   ")
    pStats1.take(10).foreach(x => {
      //pStats1: RDD[(ListBuffer[(Int, Array[Double])], ListBuffer[(Int, Array[Double])])]
      for (i <- 1 to x._1.size) {
        println("年龄：" + x._1(i - 1)._1 + " , " + x._1(i - 1)._2.mkString("||") +
           "  经验: " + x._2(i - 1)._1 + " , " + x._2(i - 1)._2.mkString("||"))
      }
    })


    /**
      * 功能6：NBA球员年龄值及经验值统计分析
      */

    //extract out the age list
    val pStats2: RDD[(Int, Array[Double])] = pStats1.flatMap { case (x, y) => x }

    //create age data frame
    val dfAge: DataFrame = processStatsAgeOrExperience(pStats2, "age")
    dfAge.show()
    //save as table
    dfAge.createOrReplaceTempView("Age")

    //extract out the experience list
    val pStats3: RDD[(Int, Array[Double])] = pStats1.flatMap { case (x, y) => y }

    //create experience dataframe
    val dfExperience: DataFrame = processStatsAgeOrExperience(pStats3, "Experience")
    // dfExperience.show()
    //save as table
    dfExperience.createOrReplaceTempView("Experience")

    pStats1.unpersist()


    sc.stop()

    // 东西太多了，这谁看得懂？
  }
}
