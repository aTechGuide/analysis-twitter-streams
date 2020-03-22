package guide.atech.analysis

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap
import guide.atech.input.TwitterReceiver
import guide.atech.nlp.SentimentType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

import scala.collection.JavaConverters._

object SentimentAnalysis {

  def covidSentiments (implicit ssc: StreamingContext): DStream[String] = {

    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)

    twitterStream
      .map(status => status.getText)
      .filter(tweet => tweet.contains("covid") || tweet.contains("corona"))
      .map { tweet =>

        val sentiment = detectSentiment(tweet) // a single "marker"
        s"[Sentiment = $sentiment] $tweet"
      }

  }


  def readTwitterWithSentiments(implicit ssc: StreamingContext): DStream[String] = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)

    twitterStream.map { status =>

      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val text = status.getText
      val sentiment = detectSentiment(text) // a single "marker"

      s"User $username ($followers follower) says$sentiment: $text"

    }
  }

  private def createNLPProps() = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  private def detectSentiment(message: String): SentimentType = {
    val pipeline = new StanfordCoreNLP(createNLPProps())
    val annotation = pipeline.process(message) // all the scores attache to this message

    // split the text into sentiments and attach the scores to each
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala
    val sentiments = sentences.map {sentence: CoreMap =>

      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      // convert the score to a double to each sentence
      RNNCoreAnnotations.getPredictedClass(tree).toDouble
    }

    // average out all sentiments detected in this text
    val averageSentiment = if (sentiments.isEmpty) -1 // Not understood
    else sentiments.sum / sentiments.length

    SentimentType.fromScore(averageSentiment)
  }

}
