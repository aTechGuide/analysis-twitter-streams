package guide.atech

import guide.atech.analysis.{SentimentAnalysis, TweetAnalysis}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterApp {

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession
    implicit val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(1))

//    val tweets = TweetAnalysis.readTwitter
//    tweets.print()

//    val length = TweetAnalysis.getAverageTweetLength
//    length.print()

//    val popularHashtag = TweetAnalysis.computeMostPopularHashtag
//    popularHashtag.print()

//    val tweets = SentimentAnalysis.readTwitterWithSentiments
//    tweets.print()

    val covidTweets = SentimentAnalysis.covidSentiments
    covidTweets.print()

    // for `reduceByKeyAndWindow` we need checkpoint
    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()

  }

  private def createSparkSession = {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}
