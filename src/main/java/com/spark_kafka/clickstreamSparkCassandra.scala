package com.spark_kafka

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import com.spark_kafka.Utilities._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soumyaka on 10/18/2016.
  */
object clickstreamSparkCassandra {
  def main(args: Array[String]): Unit = {

    //val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))

    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "DIN16000309")
      .setAppName("cass-test")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    setupLogging()

    val kafkaParams = Map("metadata.broker.list" -> "DIN16000309:9092")

    //topics is the set to which this Spark instance will listen.
    val topics = List("test-input").toSet

    //the address of kafka brokers and the topic to which this Spark instance will write
    val kafkaOutputBrokers = "DIN16000309:9092"
    val kafkaOutputTopic = "test-output1"
    val windowSec = 20
    val keySpaceName = "rajsarka"
    val tableName = "log_table"
    //lines is a Dstream. It reads from the given kafkaParams and topics
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    //customDstream takes only the required fields from "lines"
    val customDstream = lines.map(line => {
      val passedArr = line._2.split(",")
      val dateTime = passedArr(0).toLong
      val userID = passedArr(1)
      val sessionID = passedArr(2).toInt
      val referralPage = shortenedReferralPagesMap(passedArr(3))
      val landingPage = shortenedLandingPagesMap(passedArr(4))
      val timeSpent = passedArr(5).toDouble
      val county = passedArr(6)
      val city = passedArr(7)
      val latitude = passedArr(8)
      val longitude = passedArr(9)
      val age = passedArr(10).toInt
      (dateTime, userID, sessionID, referralPage, landingPage, timeSpent, county, city, latitude, longitude, age)
    })

    //customDstream.print()

    writeToLogTable(customDstream, keySpaceName, tableName)

    //Start the application
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()

  }

  def writeToLogTable(customDstream: DStream[(Long, String, Int, String, String, Double, String, String, String, String, Int)],
                      keySpaceName: String,
                      tableName: String): Unit = {

    customDstream.saveToCassandra(keySpaceName, tableName,
      SomeColumns("datetime", "user_id", "session_id", "ref_page", "land_page",
        "time_spent", "county", "city", "latitude", "longitude", "age"))
  }
}
