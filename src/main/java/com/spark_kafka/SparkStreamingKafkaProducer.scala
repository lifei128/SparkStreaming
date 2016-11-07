package com.spark_kafka

import com.spark_kafka.Utilities._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by soumyaka on 10/5/2016.
  */
object SparkStreamingKafkaProducer {
  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))

    setupLogging()

    val kafkaParams = Map("metadata.broker.list" -> "DIN16000309:9092")

    val topics = List("test-input").toSet

    val kafkaOutputBrokers = "DIN16000309:9092"
    val kafkaOutputTopic = "test-output1"

    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    //lines.print()


    lines.foreachRDD(rdd => {
      System.out.println("# events = " + rdd.count())

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

    //Start the application
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
