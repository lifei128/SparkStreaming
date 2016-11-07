import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Soumyakanti on 23-10-2016.
  */
object transformLogTable {
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
    import spark.implicits._

    val keyspace = "rajsarka"
    val table = "log_table"

    val landingPageECRMap = Map(
      "featuresandbenefits" -> "E",
      "discountsandbenfits" -> "E",
      "coverageoptions" -> "E",
      "getquotes" -> "C",
      "userdetails" -> "C",
      "onlinequotes" -> "C",
      "getcalled" -> "C",
      "claims" -> "R",
      "articles" -> "E",
      "videos" -> "E",
      "FAQ" -> "E",
      "e" -> "E",
      "blog" -> "E"
    )

    val t1 = System.nanoTime()

    val logTable = sqlc.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load()

    val something = logTable.map(row => {
      val landPageArr = row.getAs[String]("land_page").split("/")
      val timeSpent = row.getAs[Double]("time_spent")
      val userID = row.getAs[String]("user_id")
      val category = landPageArr(1)
      val ECR = landingPageECRMap(landPageArr(2))
      if (category == "vehicleinsurance") {
        ((userID, ECR + "_V"), (timeSpent, 1))
      } else if (category == "homeownerinsurance") {
        ((userID, ECR + "_H"), (timeSpent, 1))
      } else if (category == "condoinsurance") {
        ((userID, ECR + "_C"), (timeSpent, 1))
      } else if (category == "floodinsurance") {
        ((userID, ECR + "_F"), (timeSpent, 1))
      } else if (category == "rentersinsurance") {
        ((userID, ECR + "_R"), (timeSpent, 1))
      } else {
        ((userID, ECR + "_O"), (timeSpent, 1))
      }
    }).rdd
      //((tup1, tup2, tup3, tup4, tup5, tup6), (tup7, tup8, tup9, tup11, tup12))
      .reduceByKey((tup1, tup2) => {
      (tup1._1 + tup2._1, tup1._2 + tup2._2)
    })
      .map(tup => (tup._1._1, (tup._1._2, tup._2._1, tup._2._2)))
      .groupByKey()
      //.toDF("user_id", "details")
      .mapValues(_.toList)
      .map(tup => {
        val user = tup._1
        val pageList = tup._2

        val sortedArr = listToTuple(user, pageList)
        (user, (sortedArr(0)._2._1, sortedArr(0)._2._2),
          (sortedArr(1)._2._1, sortedArr(1)._2._2),
          (sortedArr(2)._2._1, sortedArr(2)._2._2),
          (sortedArr(3)._2._1, sortedArr(3)._2._2),
          (sortedArr(4)._2._1, sortedArr(4)._2._2),
          (sortedArr(5)._2._1, sortedArr(5)._2._2),
          (sortedArr(6)._2._1, sortedArr(6)._2._2),
          (sortedArr(7)._2._1, sortedArr(7)._2._2),
          (sortedArr(8)._2._1, sortedArr(8)._2._2),
          (sortedArr(9)._2._1, sortedArr(9)._2._2),
          (sortedArr(10)._2._1, sortedArr(10)._2._2),
          (sortedArr(11)._2._1, sortedArr(11)._2._2),
          (sortedArr(12)._2._1, sortedArr(12)._2._2),
          (sortedArr(13)._2._1, sortedArr(13)._2._2),
          (sortedArr(14)._2._1, sortedArr(14)._2._2),
          (sortedArr(15)._2._1, sortedArr(15)._2._2))
      })



    something.saveToCassandra(keyspace, tableName = "user_table",
      SomeColumns("user_id", "c_c", "c_f", "c_h", "c_r", "c_v",
        "e_c", "e_f", "e_h", "e_o", "e_r", "e_v",
        "r_c", "r_f", "r_h", "r_r", "r_v"))


    val t2 = System.nanoTime()

    println((t2 - t1) / 1000000.0 + " milliseconds")


  }

  def listToTuple(user: String, pageList: List[(String, Double, Int)]): Array[(String, (Double, Int))] = {
    val columnList = List("E_V", "E_H", "E_C", "E_R", "E_F", "E_O", "C_V", "C_H", "C_C", "C_R", "C_F", "R_V", "R_H", "R_C", "R_R", "R_F")
    val listSet = pageList.map(tup => tup._1).toSet
    //val listEle = pageList.map(tup => (tup._1, (tup._2, tup._3)))
    val remainingColumns = columnList.toSet.diff(listSet).toArray
    val arr = columnList.map(str => {
      if (remainingColumns.contains(str)) (str, (0.0, 0))
      else {
        returnTime_Count(str, pageList)
      }
    }).toArray
      .sortBy(_._1)

    arr
  }

  def returnTime_Count(str: String, pageList: List[(String, Double, Int)]): (String, (Double, Int)) = {
    val result = pageList.filter(tup => tup._1 == str)(0)
    (result._1, (result._2, result._3))
  }

  /*  def updateUserTable(spark:SparkSession, updateRow: Seq[Row]): Unit = {
      val updateRdd = spark.sparkContext.parallelize(updateRow)
      val tblStruct = new StructType(
        Array(StructField("user_id", TupleType, nullable = false),
          StructField("c_f", TupleType, nullable = false),
          StructField("c_h", TupleType, nullable = false),
          StructField("c_r", TupleType, nullable = false),
          StructField("c_v", TupleType, nullable = false),
          StructField("e_c", TupleType, nullable = false),
          StructField("e_f", TupleType, nullable = false),
          StructField("e_h", TupleType, nullable = false),
          StructField("e_o", TupleType, nullable = false),
          StructField("e_r", TupleType, nullable = false),
          StructField("e_v", TupleType, nullable = false),
          StructField("r_c", TupleType, nullable = false),
          StructField("r_f", TupleType, nullable = false),
          StructField("r_h", TupleType, nullable = false),
          StructField("r_r", TupleType, nullable = false),
          StructField("r_v", TupleType, nullable = false)
        ))

      val updateDf  = spark.sqlContext.createDataFrame(updateRdd, tblStruct)

      updateDf.write.format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "rajsarka", "table" -> "user_table"))
        .mode("append")
        .save()
    }*/

}
