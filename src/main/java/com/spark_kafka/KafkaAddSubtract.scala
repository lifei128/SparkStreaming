// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os

package com.spark_kafka

import java.util.concurrent.atomic.AtomicInteger

import com.spark_kafka.Utilities._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaAddSubtract {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "KafkaExample", Milliseconds(1000))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092, DIN16000602:9092, DIN16002057:9092")
    // List of topics you want to listen for from Kafka
    val topics = List("fitbit").toSet
    // Create our Kafka stream, which will contain (topic,message) pairs. We tack a 
    // map(_._2) at the end in order to only get the messages, which contain individual
    // lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    /*// Extract the request field from each log line
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    
    // Extract the URL from the request
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    
    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()*/
    var runningSum = new AtomicInteger(0)
    var numOfAdditions = new AtomicInteger(0)
    var numOfSubtractions = new AtomicInteger(0)

    /*lines.foreachRDD( (rdd, time) => {
      println(s"=======$time=======")
      for(i <- rdd.collect()){
        val operation = i._1.trim
        val value = i._2.trim.toInt
        if(operation == "add"){
          runningSum.getAndAdd(value)
          //println("After adding " + value + ": " + runningSum.get())
          println(runningSum.get())
          numOfAdditions.getAndAdd(1)
        } else if (operation == "sub"){
          runningSum.getAndAdd(-value)
          //println("After subtracting " + value + ": " + runningSum.get())
          println(runningSum.get())
          numOfSubtractions.getAndAdd(1)
        }
      }
      println("Additions: " + numOfAdditions)
      println("Subtractions: " + numOfSubtractions)

    })*/

    val posNegNum = lines.map(l => {
      val operation = l._1.trim
      val value = l._2.trim.toInt
      if (operation == "add") {
        value
      } else if (operation == "sub") {
        -value
      } else 0
    })

    val sum = posNegNum.reduce(_ + _)

    sum.print()


    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

