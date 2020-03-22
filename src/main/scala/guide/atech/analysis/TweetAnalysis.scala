package guide.atech.analysis

import guide.atech.input.TwitterReceiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object TweetAnalysis {

  def readTwitter(implicit ssc: StreamingContext): DStream[String] = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)

    twitterStream.map { status =>

      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val text = status.getText

      s"User $username ($followers follower) says: $text"

    }
  }

  def getAverageTweetLength(implicit ssc: StreamingContext): DStream[Double] = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)

    twitterStream
      .map(status => status.getText)
      .map(text => text.length)
      .map(len => (len, 1))
      .reduceByWindow((tup1, tup2) => (tup1._1 + tup2._1, tup1._2 + tup2._2), Seconds(5), Seconds(5))
      .map { megaTuple =>
        val tweetLengthSum = megaTuple._1
        val tweetCount = megaTuple._2

        tweetLengthSum * 1.0 / tweetCount
      }
  }

  def computeMostPopularHashtag(implicit ssc: StreamingContext): DStream[(String, Int)] = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)

    twitterStream
      .flatMap(_.getText.split(" "))
      .filter(_.startsWith("#"))
      .map(hashtag => (hashtag, 1))
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        Seconds(60),
        Seconds(10)
      )
      .transform(rdd => rdd.sortBy(tuple => - tuple._2))
  }

}
