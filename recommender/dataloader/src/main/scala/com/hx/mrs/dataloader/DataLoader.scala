package com.hx.mrs.dataloader

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created  on 2019/04/07.
  */

/*    movie数据初始化格式
  * 1^
  * Toy Story (1995)^
  * 81 minutes^March 20, 2001^
  * 1995 ^
  *  English^
  * Adventure|Animation|Children|Comedy|Fantasy^
  *  Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn|John Ratzenberger|Annie Potts|John Morris|Erik von Detten|Laurie Metcalf|R. Lee Ermey|Sarah Freeman|Penn Jillette|Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn^
  *  John Lasseter^

    */

/*
* rating数据初始化格式
*   1,用户id
*   31,电影id
*   2.5,评分
*    1260759144评分时间

* */

/** tags数据初始化格式
  * 15, 用户id
  * 339,电影id
  * sandra 'boring' bullock,标签具体内容
  * 1138537770打标签的时间啊
  */
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String,
                 val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
  * 连接MongoDB的配置
  * @param url 主机地址
  * @param db 数据库
  */
case class MongoConfig(val url:String,val db:String);

/**
  * es的链接配置
  * @param httpHosts 主机地址
  * @param transportHosts
  * @param index 索引
  * @param clustername 集群名称
  */
case class ElasticSearchConfig(val httpHosts:String, val transportHosts:String, val index:String, val clustername:String)

object DataLoader {

  val MOVIE_DATA_PATH="D:\\IdeaProjects\\mrs\\recommender\\dataloader\\src\\main\\resources\\aboutmovie\\movies.csv"
  val RATING_DATA_PATH="D:\\IdeaProjects\\mrs\\recommender\\dataloader\\src\\main\\resources\\aboutmovie\\ratings.csv"
  val TAG_DATA_PATH="D:\\IdeaProjects\\mrs\\recommender\\dataloader\\src\\main\\resources\\aboutmovie\\tags.csv"
  val MONGODB_MOVIE_COLLECTION="Movie"
  val MONGODB_RATING_COLLECTION="Rating"
  val MONGODB_TAG_COLLECTION="Tag"
  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.cores" -> "local[3]",
      "mongo.uri" -> "mongodb://192.168.25.103/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "118.89.26.121:9200",
      "es.transportHosts" -> "118.89.26.121:9300",
      "es.index" ->"recommender",
      "es.cluster.name" ->"elasticsearch"
    )
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //============开始加载数据=======================
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    //转换为DataFrame
    val movieDF=movieRDD.map(item =>{
      val attr = item.split("\\^")
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF=ratingRDD.map(item =>{
      val attr = item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()
    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF=tagRDD.map(item =>{
      val attr=item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()
    //============加载数据结束=======================

    implicit val mongoConfig=MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)

    //将数据保存到MongoDB中
    storeDataToMongoDB(movieDF,ratingDF,tagDF)

    //将数据保存到ElasticSearch中
   // storeDataToEs()


    //stop
    spark.stop()

  }

  def storeDataToMongoDB(movieDF:DataFrame,ratingDF:DataFrame,tagDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //建立与MongoDB的链接
    val mongoClient=MongoClient(MongoClientURI(mongoConfig.url))
    //存在相关数据库，删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()
    //写入MongoDB
    movieDF.write.option("uri",mongoConfig.url)
                 .option("collection",MONGODB_MOVIE_COLLECTION)
                 .mode("overwrite")
                 .format("com.mongodb.spark.sql")
                 .save()
    ratingDF.write.option("uri",mongoConfig.url)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF.write.option("uri",mongoConfig.url)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //创建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" ->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" ->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" ->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" ->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" ->1))

    //关闭连接
    mongoClient.close()

  }

  def storeDataToEs(): Unit = {

  }
}
