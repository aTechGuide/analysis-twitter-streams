# Analyse Twitter Streams

This project performs the following analysis
- Analyzing average Tweet length
- Analyzing most popular Hashtag
- Performs Sentiment Analysis on Tweets
- Performs Sentiment Analysis on Covid Tweets

## Tech Stack
- Spark Streaming
- Scala
- sbt 


## Set Up
- Create a checkpoint location `checkpoint`
- Create the file `twitter4j.properties` under `src/main/resources/` and populate it with Twitter Credentials.
  - Refer to `twitter4j.properties_sample` for field names

# References
- This project is build as part of [rockthejvm.com  spark-streaming](https://rockthejvm.com/p/spark-streaming) Course
  
  