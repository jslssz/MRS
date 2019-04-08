package com.hx.mrs.dataloader

import java.net.InetAddress

import com.hx.mrs.common._
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


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

object DataLoader {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.25.103/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "118.89.26.121",
      "es.port" -> "9200",
      "es.transportHosts" -> "118.89.26.121",
      "es.transport" -> "9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "my-application"
    )
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)
    //必须配下面第一个参数
    sparkConf.set("es.nodes.wan.only", "true")
    sparkConf.set("es.write.operation", "index")
    sparkConf.set("es.batch.size.bytes", "300000000")
    sparkConf.set("es.batch.size.entries", "10000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set("es.batch.write.retry.count", "50")
    sparkConf.set("es.batch.write.retry.wait", "500")
    sparkConf.set("es.http.retries", "50")
    sparkConf.set("es.http.timeout", "100s")
    sparkConf.set("es.action.heart.beat.lead", "50")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //============开始加载数据=======================
    val movieRDD = spark.sparkContext.textFile(Constant.MOVIE_DATA_PATH)
    //转换为DataFrame
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(Constant.RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()
    val tagRDD = spark.sparkContext.textFile(Constant.TAG_DATA_PATH)
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()
    //============加载数据结束=======================

    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)
    //============将数据保存到MongoDB中========
    // storeDataToMongoDB(movieDF, ratingDF, tagDF)

    //将数据保存到ElasticSearch中
    import org.apache.spark.sql.functions._

    //先处理Tag
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags"))
    //合并
    val movieDFWithTags = movieDF.join(newTag, Seq("mid", "mid"), "left").select("mid", "name", "descri", "timelong", "issue", "shoot", "language", "genres", "actors", "directors", "tags")
    //存入

    implicit val elasticSearchConfig = ElasticSearchConfig(config.get("es.httpHosts").get,
      config.get("es.transportHosts").get,
      config.get("es.index").get,
      config.get("es.cluster.name").get,
      config.get("es.port").get,
      config.get("es.transport").get
    )

    storeDataToEs(movieDFWithTags)


    //stop
    spark.stop()

  }

  def storeDataToMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //建立与MongoDB的链接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.url))
    //存在相关数据库，删除
    mongoClient(mongoConfig.db)(Constant.MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(Constant.MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(Constant.MONGODB_TAG_COLLECTION).dropCollection()
    //写入MongoDB
    movieDF.write.option("uri", mongoConfig.url)
      .option("collection", Constant.MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write.option("uri", mongoConfig.url)
      .option("collection", Constant.MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF.write.option("uri", mongoConfig.url)
      .option("collection", Constant.MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //创建索引
    mongoClient(mongoConfig.db)(Constant.MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(Constant.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(Constant.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(Constant.MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(Constant.MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭连接
    mongoClient.close()

  }

  def storeDataToEs(movieDF: DataFrame)(implicit esConf: ElasticSearchConfig): Unit = {
    //下面这句话非常重要，否则会报错
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    //创建客户端
    val settings: Settings = Settings.builder.put("cluster.name", esConf.clustername).build
    val client = new PreBuiltTransportClient(settings)
    client.addTransportAddress(new TransportAddress(InetAddress.getByName(esConf.transportHosts), esConf.estansport.toInt))
    //把索引是否存在的判断略去
    client.admin.indices.prepareCreate(esConf.index).get

    movieDF.write.option("es.nodes", esConf.httpHosts)
      .option("es.port", esConf.esport)
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConf.index + "/" + Constant.ES_MOVIE_INDEX)

  }
}
