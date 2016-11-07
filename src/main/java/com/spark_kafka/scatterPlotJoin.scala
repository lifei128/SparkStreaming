package com.spark_kafka

import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by soumyaka on 10/24/2016.
  */
object scatterPlotJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "DIN16000309")
      .set("spark.sql.warehouse.dir",
        "file:///D:/global-coding-env/IdeaProjec_c_TS/sparkTest/spark-warehouse")
      .setAppName("cass-test")
      .setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlc = spark.sqlContext
    Logger.getRootLogger().setLevel(Level.ERROR)

    val keyspace = "rajsarka"
    val table = "log_table"

    val t1 = System.nanoTime()

    val logTable = sqlc.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load().select("user_id", "age").distinct()

    val user_Table = sqlc.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "user_table", "keyspace" -> keyspace))
      .load()

    val joinedTable = logTable.join(user_Table, logTable("user_id") === user_Table("user_id"))
      .drop(user_Table("user_id"))


    val combinedRDD = joinedTable.select("user_id", "age", "e_v", "c_v", "r_v", "e_c", "c_c", "r_c", "e_f", "c_f", "r_f",
      "e_h", "c_h", "r_h", "e_r", "c_r", "r_r", "e_o").rdd
      .map(row => {
        val user = row.getString(0)
        val age = row.getInt(1)
        val Array(e_v_Ts, e_v_Count) = row.get(2).toString.split(",")
        val e_V = (e_v_Ts.substring(1).toDouble, e_v_Count.substring(0, e_v_Count.length - 1).toInt)
        val Array(c_v_Ts, c_v_Count) = row.get(3).toString.split(",")
        val c_V = (c_v_Ts.substring(1).toDouble, c_v_Count.substring(0, c_v_Count.length - 1).toInt)
        val Array(r_v_Ts, r_v_Count) = row.get(4).toString.split(",")
        val r_V = (r_v_Ts.substring(1).toDouble, r_v_Count.substring(0, r_v_Count.length - 1).toInt)


        val Array(e_c_Ts, e_c_Count) = row.get(5).toString.split(",")
        val e_C = (e_c_Ts.substring(1).toDouble, e_c_Count.substring(0, e_c_Count.length - 1).toInt)
        val Array(c_c_TS, c_c_Count) = row.get(6).toString.split(",")
        val c_C = (c_c_TS.substring(1).toDouble, c_c_Count.substring(0, c_c_Count.length - 1).toInt)
        val Array(r_c_Ts, r_c_Count) = row.get(7).toString.split(",")
        val r_C = (r_c_Ts.substring(1).toDouble, r_c_Count.substring(0, r_c_Count.length - 1).toInt)


        val Array(e_f_Ts, e_f_Count) = row.get(8).toString.split(",")
        val e_F = (e_f_Ts.substring(1).toDouble, e_f_Count.substring(0, e_f_Count.length - 1).toInt)
        val Array(c_f_TS, c_f_Count) = row.get(9).toString.split(",")
        val c_F = (c_f_TS.substring(1).toDouble, c_f_Count.substring(0, c_f_Count.length - 1).toInt)
        val Array(r_f_Ts, r_f_Count) = row.get(10).toString.split(",")
        val r_F = (r_f_Ts.substring(1).toDouble, r_f_Count.substring(0, r_f_Count.length - 1).toInt)


        val Array(e_h_Ts, e_h_Count) = row.get(11).toString.split(",")
        val e_H = (e_h_Ts.substring(1).toDouble, e_h_Count.substring(0, e_h_Count.length - 1).toInt)
        val Array(c_h_TS, c_h_Count) = row.get(12).toString.split(",")
        val c_H = (c_h_TS.substring(1).toDouble, c_h_Count.substring(0, c_h_Count.length - 1).toInt)
        val Array(r_h_Ts, r_h_Count) = row.get(13).toString.split(",")
        val r_H = (r_h_Ts.substring(1).toDouble, r_h_Count.substring(0, r_h_Count.length - 1).toInt)


        val Array(e_r_Ts, e_r_Count) = row.get(14).toString.split(",")
        val e_R = (e_r_Ts.substring(1).toDouble, e_r_Count.substring(0, e_r_Count.length - 1).toInt)
        val Array(c_r_TS, c_r_Count) = row.get(15).toString.split(",")
        val c_R = (c_r_TS.substring(1).toDouble, c_r_Count.substring(0, c_r_Count.length - 1).toInt)
        val Array(r_r_Ts, r_r_Count) = row.get(16).toString.split(",")
        val r_R = (r_r_Ts.substring(1).toDouble, r_r_Count.substring(0, r_r_Count.length - 1).toInt)


        val Array(e_o_Ts, e_o_Count) = row.get(17).toString.split(",")
        val e_O = (e_o_Ts.substring(1).toDouble, e_o_Count.substring(0, e_o_Count.length - 1).toInt)

        val result = (user, age, (e_V._1 + c_V._1 + r_V._1, e_V._2 + c_V._2 + r_V._2),
          (e_C._1 + c_C._1 + r_C._1, e_C._2 + c_C._2 + r_C._2),
          (e_F._1 + c_F._1 + r_F._1, e_F._2 + c_F._2 + r_F._2),
          (e_H._1 + c_H._1 + r_H._1, e_H._2 + c_H._2 + r_H._2),
          (e_R._1 + c_R._1 + r_R._1, e_R._2 + c_R._2 + r_R._2),
          (e_O._1, e_O._2))

        result
      }).cache()

    /* val aggregatedRDD = combinedRDD
       .map(tup => {
         (tup._3, tup._4, tup._5, tup._6, tup._7, tup._8)
       })
       .reduce((tup1, tup2) => {
         ((tup1._1._1 + tup2._1._1, tup1._1._2 + tup2._1._2),
           (tup1._2._1 + tup2._2._1, tup1._2._2 + tup2._2._2),
           (tup1._3._1 + tup2._3._1, tup1._3._2 + tup2._3._2),
           (tup1._4._1 + tup2._4._1, tup1._4._2 + tup2._4._2),
           (tup1._5._1 + tup2._5._1, tup1._5._2 + tup2._5._2),
           (tup1._6._1 + tup2._6._1, tup1._6._2 + tup2._6._2)
           )
       })*/


    val numUsers = combinedRDD.count().toDouble
    val aggregatedRDD = combinedRDD
      .map(tup => {
        (1, (tup._3, tup._4, tup._5, tup._6, tup._7, tup._8))
      })
      .reduceByKey((tup1, tup2) => {
        ((tup1._1._1 + tup2._1._1, tup1._1._2 + tup2._1._2),
          (tup1._2._1 + tup2._2._1, tup1._2._2 + tup2._2._2),
          (tup1._3._1 + tup2._3._1, tup1._3._2 + tup2._3._2),
          (tup1._4._1 + tup2._4._1, tup1._4._2 + tup2._4._2),
          (tup1._5._1 + tup2._5._1, tup1._5._2 + tup2._5._2),
          (tup1._6._1 + tup2._6._1, tup1._6._2 + tup2._6._2)
          )
      })
      .map(tup => {
        (1, (tup._2._1._1 / numUsers, tup._2._1._2.toDouble / numUsers),
          (tup._2._2._1 / numUsers, tup._2._2._2.toDouble / numUsers),
          (tup._2._3._1 / numUsers, tup._2._3._2.toDouble / numUsers),
          (tup._2._4._1 / numUsers, tup._2._4._2.toDouble / numUsers),
          (tup._2._5._1 / numUsers, tup._2._5._2.toDouble / numUsers),
          (tup._2._6._1 / numUsers, tup._2._6._2.toDouble / numUsers))
      })


    combinedRDD.saveToCassandra(keyspace, tableName = "scatter_plot_table",
      SomeColumns("user_id", "age", "v", "c", "f", "h", "r", "o"))

    //val result = sqlc.createDataset(aggregatedRDD)

    aggregatedRDD.saveToCassandra(keyspace, tableName = "aggregated_table",
      SomeColumns("useless", "v", "c", "f", "h", "r", "o"))

    val t2 = System.nanoTime()

    println((t2 - t1) / 1000000.0 + " milliseconds")
    //combinedRDD.take(100).foreach(println)
  }

}
