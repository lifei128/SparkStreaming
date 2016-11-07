package com.spark_kafka

import com.spark_kafka.Utilities._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by soumyaka on 10/6/2016.
  */
object clickstreamSpark {
  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))

    setupLogging()

    val kafkaParams = Map("metadata.broker.list" -> "DIN16000309:9092")

    //topics is the set to which this Spark instance will listen.
    val topics = List("test-input").toSet

    //the address of kafka brokers and the topic to which this Spark instance will write
    val kafkaOutputBrokers = "DIN16000309:9092"
    val kafkaOutputTopic = "test-output1"
    val windowSec = 20

    //lines is a Dstream. It reads from the given kafkaParams and topics
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    //customDstream takes only the required fields from "lines"
    val customDstream = lines.map(line => {
      val landingPage = line._2.split(",")(6)
      val time = line._2.split(",")(7).toDouble
      val referralPage = line._2.split(",")(5)
      (landingPage, time, referralPage)
    })

    //These methods apply business logic to the customDstream, connect to "kafkaOutputBrokers"
    //and produce messages on the given "kafkaOutputTopic".
    //NOTE: kafkaOutputTopic is the name of the respective method. Follow this convention to avoid confusion.


    //averageTimePerPage(customDstream, windowSec, kafkaOutputTopic = "averageTimePerPage", kafkaOutputBrokers)

    averageTime(customDstream, windowSec, kafkaOutputTopic = "averageTime", kafkaOutputBrokers)

    usersPerCategory(customDstream, insuranceCategoryMap, windowSec, kafkaOutputTopic = "usersPerCategory", kafkaOutputBrokers)

    bounceRate(customDstream, windowSec, kafkaOutputTopic = "bounceRate", kafkaOutputBrokers)

    hitsByMarketingChannels(customDstream, referralCategoryMap, windowSec, kafkaOutputTopic = "hitsByMarketingChannels", kafkaOutputBrokers)

    pagesByBounceRate(customDstream, shortenedLandingPagesMap, windowSec, kafkaOutputTopic = "pagesByBounceRate", kafkaOutputBrokers)

    /*averageTimePerPage.foreachRDD(rdd => {        //code to create each rdd into DF and then into tempTable
      val df = rdd.toDF("line", "averageTime")      //This can be used with JDBC server to query
      df.createOrReplaceTempView("avgTimePerLine")
    })*/

    //Start the application
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

  /*
   *Calculates average time spent given a window and slide duration
   */
  def averageTime(customDstream: DStream[(String, Double, String)], windowSec: Int,
                  kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {
    val averageTime = customDstream
      .map(line => (line._2, 1))
      .reduceByWindow(
        (tup1, tup2) => (tup1._1 + tup2._1, tup1._2 + tup2._2),
        (tup1, tup2) => (tup1._1 - tup2._1, tup1._2 - tup2._2),
        windowDuration = Seconds(windowSec),
        slideDuration = Seconds(2)
      )
      .map(line => "%.2f".format(line._1 / line._2))


    averageTime.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {

        val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))
        partition.foreach(record => {
          val data = record.toString
          val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
          producer.send(message)

        })
        producer.close()
      })
    })
  }

  /*
  *
  *  Calculates average time per page
  *
  * */
  def averageTimePerPage(customDstream: DStream[(String, Double, String)], windowSec: Int,
                         kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {


    val averageTimePerPage =
      customDstream
        //transform (landingPage, time, user) to (landingPage, (time, 1))
        .map(line => (line._1, (line._2, 1)))
        //We are reducing by key and window. Thus tup1 = (time1, 1), tup2 = (time2, 1)
        .reduceByKeyAndWindow(
        //reduceFunc: ((time1, 1), (time2, 1)) => (total time, total count)
        (tup1, tup2) => (tup1._1 + tup2._1, tup1._2 + tup2._2),
        //Inverse function
        (tup1, tup2) => (tup1._1 - tup2._1, tup1._2 - tup2._2),
        windowDuration = Seconds(windowSec),
        slideDuration = Seconds(2))
        //calculates average
        .map(line => (line._1, "%.2f".format(line._2._1 / line._2._2)))


    averageTimePerPage.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {

        val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))
        partition.foreach(record => {
          val data = record.toString
          val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
          producer.send(message)

        })
        producer.close()
      })
    })
  }

  /*
  *
  * Counts number of users by landing page categories
  * Message is sent as a string of sorted tuples separated by ", "
  * Tuples are sorted alphabetically by Category names
  */
  def usersPerCategory(customDstream: DStream[(String, Double, String)], insuranceCategoryMap: Map[String, String],
                       windowSec: Int, kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {
    val sortedUsersPerCategory = customDstream
      .filter(line => insuranceCategoryMap.contains(line._1))
      .map(line => (insuranceCategoryMap(line._1.trim), 1))
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        windowDuration = Seconds(windowSec),
        slideDuration = Seconds(2)
      )

    val setOfCategory = Set("Condo Insurance", "Vehicle Insurance", "Home Owner Insurance", "Renters Insurance", "Flood Insurance", "Umbrella Insurance")
    //sortedUsersPerCategory.print()
    sortedUsersPerCategory.foreachRDD(rdd => {
      val keySet = rdd.map(line => line._1).collect().toSet
      val diffArr = setOfCategory.diff(keySet).map(s => (s, 0)).toArray
      val wholeArr = rdd.collect() ++ diffArr
      val sortedArr = wholeArr.sortBy(_._1)
      var customData = ""

      for (i <- sortedArr) {
        customData += i + ", "
      }

      val data = customData.slice(0, customData.length - 2)

      val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))

      val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
      producer.send(message)

      producer.close()

    })
  }

  /*
   * Calculates bounce rate. If a user spends less than 2 minutes on a page, it is considered
   * as a bounced case.
   */
  def bounceRate(customDstream: DStream[(String, Double, String)], windowSec: Int,
                 kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {

    val numberOfBounces = customDstream
      .map(line => {
        if (line._2 < 2) (1, 1) else (1, 0)
      })
      .reduceByWindow(
        (tup1, tup2) => (tup1._1 + tup2._1, tup1._2 + tup2._2),
        (tup1, tup2) => (tup1._1 - tup2._1, tup1._2 - tup2._2),
        windowDuration = Seconds(windowSec),
        slideDuration = Seconds(2)
      )
      .map(line => "%.2f".format((line._2.toDouble / line._1.toDouble) * 100.0))


    numberOfBounces.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {

        val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))
        partition.foreach(record => {
          val data = record.toString
          val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
          producer.send(message)

        })
        producer.close()
      })
    })
  }

  /*
   * Counts number of users referred by referral categories
   * Message is sent as a string of sorted tuples separated by ", "
   * Tuples are sorted in descending order of count of categories
   */
  def hitsByMarketingChannels(customDstream: DStream[(String, Double, String)], referralCategoryMap: Map[String, String],
                              windowSec: Int, kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {
    val sortedCountByReferralCategory = customDstream
      .map(line => (referralCategoryMap(line._3), 1))
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        windowDuration = Seconds(windowSec),
        slideDuration = Seconds(2)
      )
    /*.map(item => item.swap)
    .transform(rdd => rdd.sortByKey(ascending = false))
    .map(item => item.swap)*/

    val setOfCategory = Set("ads", "emails", "direct", "news channels", "search engines", "social media")

    sortedCountByReferralCategory.foreachRDD(rdd => {
      val keySet = rdd.map(line => line._1).collect().toSet
      val diffArr = setOfCategory.diff(keySet).map(s => (s, 0)).toArray
      val wholeArr = rdd.collect() ++ diffArr
      val sortedArr = wholeArr.sortBy(_._2).reverse
      var customData = ""
      for (i <- sortedArr) {
        if (i._1 == "direct") {
          val value = (i._1, i._2 / 15)
          customData += value + ", "
        } else customData += i + ", "
      }

      val data = customData.slice(0, customData.length - 2)

      val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))

      val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
      producer.send(message)

      producer.close()

    })
  }

  /*
  * Counts bounce rate per page and sorts by maximum rate
  *
  */
  def pagesByBounceRate(customDstream: DStream[(String, Double, String)], shortenedLandingPagesMap: Map[String, String],
                        windowSec: Int, kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {

    val sortedPagesByBounceRate = customDstream
      .map(line => {
        if (line._2 < 2.toDouble) (shortenedLandingPagesMap(line._1), (1.0, 1.0))
        else (shortenedLandingPagesMap(line._1), (1.0, 0.0))
      })
      .reduceByKeyAndWindow(
        (tup1, tup2) => (tup1._1 + tup2._1, tup1._2 + tup2._2),
        (tup1, tup2) => (tup1._1 - tup2._1, tup1._2 - tup2._2),
        windowDuration = Seconds(windowSec),
        slideDuration = Seconds(2)
      )
      .map(line => {
        if (line._2._1 > 0) (line._1, (line._2._2 / line._2._1) * 100.0)
        else (line._1, 0.0)
      })
      .map(item => item.swap)
      .transform(rdd => rdd.sortByKey(false))
      .map(item => item.swap)
      .map(line => (line._1, "%.2f".format(line._2)))


    sortedPagesByBounceRate.foreachRDD(rdd => {
      val resultArr = rdd.take(10)

      var customData = ""

      for (i <- resultArr) {
        customData += i + ", "
      }

      val data = customData.slice(0, customData.length - 2)

      val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))

      val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
      producer.send(message)

      producer.close()
    })
  }
}
