package com.spark_kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by soumyaka on 10/24/2016.
  */
object transformUserDetailsTable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "DIN16000309")
      .set("spark.sql.warehouse.dir",
        "file:///D:/global-coding-env/IdeaProjects/sparkTest/spark-warehouse")
      .setAppName("cass-test")
      .setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlc = spark.sqlContext
    Logger.getRootLogger().setLevel(Level.ERROR)

    val keyspace = "rajsarka"
    val table = "user_details_table"

    val userDetailsTable = sqlc.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load().rdd

    val something = userDetailsTable.map(row => {
      val userID = row.getAs[String]("user_id")
      val pageList = row.getList[(String, Double, Int)](1)
      (pageList)
    })

    something.take(10).foreach(println)
  }
}
