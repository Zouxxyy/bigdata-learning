package com.zouxxyy.spark.chap01


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 1.2节 通过 DataFrame 和 Dataset 实战电影点评系统
  */

object MovieAnalyzer2 {
  def main(args: Array[String]): Unit = {

    // 指定本地运行与程序名字
    val conf = new SparkConf().setMaster("local[*]").setAppName ("MovieAnalyzer2")
    val spark= SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val dataPath = "data/chap01/moviedata/medium/" //数据存放的目录

    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")


    /**
      * 案例一：通过 DataFrame 实现某部电影观看者中男性和女性不同年龄分别有多少人。
      */

    println("功能一，通过 DataFrame 实现某部电影观看者中男性和女性不同年龄人数")

    // 1. 构建usersDataFrame
    // 第一步：将Users的数据格式化
    val schemaForUsers = StructType(
      "UserID::Gender::Age::OccupationIDd::Zip-code".split("::")
        .map(column => StructField(column, StringType, true))
    )

    // 第二步：将每一条数据变成Row为单位的数据
    val usersRDDRows = usersRDD.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))

    // 第三步：用createDataFrame结合
    val usersDataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers)

    // 2. 构建ratingsDataFrame
    val schemaForRatings = StructType(
      "UserID::MovieID".split("::").map(column => StructField(column, StringType, true)))
      .add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)

    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim)) // 这里别忘了toDouble

    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaForRatings)

    // 3. 构建moviesDataFrame
    val schemaForMovies = StructType(
      "MovieID::Title::Genres".split("::")
        .map(column => StructField(column, StringType, true))
    )

    val moviesRDDRows = moviesRDD.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim))

    val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaForMovies)

    // 非常像在使用sql语句
    ratingsDataFrame.filter(s"MovieID = 1193")
      .join(usersDataFrame, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)

    /**
      功能一，通过 DataFrame 实现某部电影观看者中男性和女性不同年龄人数
      +------+---+-----+
      |Gender|Age|count|
      +------+---+-----+
      |     F| 45|   55|
      |     M| 50|  102|
      |     M|  1|   26|
      |     F| 56|   39|
      |     F| 50|   43|
      |     F| 18|   57|
      |     F|  1|   10|
      |     M| 18|  192|
      |     F| 25|  140|
      |     M| 45|  136|
      +------+---+-----+
       */


    /**
      * 案例二： 用 LocalTempView 实现某部电影观看荷中不同性别不同年龄分别有多少人?
      */

    println("\n用 LocalTempView 实现某部电影观看荷中不同性别不同年龄分别有多少人")
    ratingsDataFrame.createTempView("ratings") // 注册临时表
    usersDataFrame.createTempView("users")
    val sql_local = "SELECT Gender, Age, count(*) from users u join ratings as r on u.UserID = r.UserID where MovieID = 1193 group by Gender, Age"
    spark.sql(sql_local).show(10)

    /**
      用 LocalTempView 实现某部电影观看荷中不同性别不同年龄分别有多少人
      +------+---+--------+
      |Gender|Age|count(1)|
      +------+---+--------+
      |     F| 45|      55|
      |     M| 50|     102|
      |     M|  1|      26|
      |     F| 56|      39|
      |     F| 50|      43|
      |     F| 18|      57|
      |     F|  1|      10|
      |     M| 18|     192|
      |     F| 25|     140|
      |     M| 45|     136|
      +------+---+--------+
      */

    /**
      * 案例三：隐式转换、与DataFrame和RDD混编
      */

    // 隐式转换
    import spark.sqlContext.implicits._ // 还有这种操作？
    ratingsDataFrame.select("MovieID", "Rating")
      .groupBy("MovieID").avg("Rating")
      .orderBy($"avg(Rating)".desc)
      .show(10)

    /**
      +-------+-----------+
      |MovieID|avg(Rating)|
      +-------+-----------+
      |   3382|        5.0|
      |   3172|        5.0|
      |   3881|        5.0|
      |   3233|        5.0|
      |   3280|        5.0|
      |    787|        5.0|
      |   3656|        5.0|
      |   3607|        5.0|
      |    989|        5.0|
      |   1830|        5.0|
      +-------+-----------+
      only showing top 10 rows
      */

    // DataFrame和RDD混编
    ratingsDataFrame.select("MovieID", "Rating")
      .groupBy("MovieID").avg("Rating") // (MovieID, avg(Rating))
      .rdd.map(row => (row(1), row(0), row(1))) // (avg(Rating), MovieID, avg(Rating))
      .sortBy(_._1.toString.toDouble, false)
      .map(tuple => tuple._2)
      .collect.take(10).foreach(println)

    /**
      3656
      3280
      989
      787
      3607
      3172
      3233
      3881
      1830
      3382
      */

    /**
      * 案例四：通过 DataSet 实战电影点评系统案例
      */

    import spark.implicits._

    case class User(UserID:String, Gender: String, Age: String, OccupationID: String, Zip_Code: String)
    case class Rating(UserID:String, MovieID: String, Rating:Double, Timestamp: String)

    // 先把数据封装进具体类，再直接创建DataSet

//    val usersForDSRDD = usersRDD.map(_.split("::"))
//      .map(line => User(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
//    val usersDataSet = spark.createDataset[User](usersForDSRDD) // 方式1
//
//    val usersDataSet = usersDataFrame.as[User] // 方式2
//    usersDataSet.show(10)
//
//    val ratingsForDSRDD = ratingsRDD.map(_.split("::"))
//      .map(line => Rating(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
//    val ratingsDataSet = spark.createDataset[Rating](ratingsForDSRDD)
//
//    ratingsDataSet.filter(s" MovieID = 1193").join(usersDataSet, "UserID")
//      .select("Gender", "Age").groupBy("Gender", "Age").count()
//      .orderBy($"Gender".desc, $"Age").show()


    /**
      Error:(188, 49) Unable to find encoder for type User. An implicit Encoder[User] is needed to store User instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
      val usersDataSet = spark.createDataset[User](usersForDSRDD)
      不知道什么原因，学完后面再来看看
      */





    spark.stop()

  }
}
