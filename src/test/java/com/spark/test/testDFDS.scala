package com.spark.test

import com.spark_kafka.Utilities._
import org.apache.spark.sql._

/**
  * Created by soumyaka on 10/6/2016.
  */
object testDFDS {
  def main(args: Array[String]): Unit = {

    setupLogging()

    val spark = SparkSession
      .builder()
      .appName("Simple Example")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    case class Person(name: String, age: Long)
    val caseClassDS = Seq(1, 2, 3).toDS()

    println(caseClassDS)

    spark.stop()
  }
}
