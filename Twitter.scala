package twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by death on 2017/12/28.
  */
object Twitter extends App {
  // 设置JVM的系统代理
  System.setProperty("http.proxyHost", "127.0.0.1")
  System.setProperty("http.proxyPort", "1080")
  // 以本地模式测试
  val stream = new StreamingContext(new SparkConf().setMaster("local[2]").setAppName("twitter"), Seconds(5))
  val sc = stream.sparkContext
  sc.setLogLevel("ERROR")
  // 设置Twitter APP的接口Key的信息
  System.setProperty("twitter4j.oauth.consumerKey", "--------")
  System.setProperty("twitter4j.oauth.consumerSecret", "-------")
  System.setProperty("twitter4j.oauth.accessToken", "-------")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "---------")
  // 使用TwitterUtils得到一个DSteam对象
  val tweets = TwitterUtils.createStream(stream, None)
  // 测试一下是否能够正确获得数据
  val status = tweets.filter(tweet => tweet.getLang == "zh" || tweet.getLang == "zh-CN")
  status.print()
  stream.checkpoint("./check")
  stream.start()
  stream.awaitTermination()
}
