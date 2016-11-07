package com.spark_kafka

import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soumyaka on 10/18/2016.
  */
object cassandraSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("cass-test")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    Logger.getRootLogger().setLevel(Level.ERROR)

    val table = sc.cassandraTable[(String, Int)]("soumyaka", "temp")
    table.collect().foreach(println)
  }

}
