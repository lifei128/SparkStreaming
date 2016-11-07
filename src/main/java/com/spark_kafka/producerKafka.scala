package com.spark_kafka

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by rajsarka on 10/5/2016.
  */
object producerKafka {
  def main(args: Array[String]): Unit = {
    val kafkaBrokers = "DIN16000309:9092"
    val kafkaOpTopic = "test-input"
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val bufferedSource = scala.io.Source.fromFile("//DIN16000602/ClickStreamData/rajsarka/clickStream_data_Generator/generated_dataset/Date-2016-09-28.csv")
    var countLines = 0
    for (line <- bufferedSource.getLines()) {
      if (countLines % 5 == 0) {
        Thread.sleep(100)
        //        break()
      }
      countLines = countLines + 1
      //      val Array(f1, f2, f3, f4, f5, f6, f7, f8) = line.split(",").map(_.trim)
      val message = new ProducerRecord[String, String](kafkaOpTopic, line)
      producer.send(message)
    }


    //    var running_sum = 0
    //    while (true) {
    //      for (i <- 1.to(100)) {
    //
    //        val data = (System.currentTimeMillis()).toString
    //        // As as debugging technique, users can write to DBFS to verify that records are being written out
    //        // dbutils.fs.put("/tmp/test_kafka_output",data,true)
    //        val message = new ProducerRecord[String, String](kafkaOpTopic, data)
    //        producer.send(message)
    //        //      running_sum=running_sum + 1
    //        Thread.sleep(200)
    //      }
    //    }
    producer.close()
  }

}
