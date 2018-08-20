package com.huawei.dli

import java.sql.{Connection, Driver, DriverManager}

import com.google.gson.JsonParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dis.{ConsumerStrategies, DISUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

case class DwsInfo(url: String, userName: String, password: String, driver: String)

object DisToDws {
  val log = LoggerFactory.getLogger("veolia")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Spark streaming DIS example")

    log.info("Start DIS Spark Streaming demo.")

    //
    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets, duration)
    = ("https://dis.cn-north-1.myhwclouds.com:20004", "cn-north-1",
      "XXX", "XXX", "769b7a1e32ea43b3ab8362070556d81c",
      "dis-veolia", "LATEST", "10")

    val (dwsurl, username, dwspassword, driver) = ("jdbc:postgresql://veolia-dws.dws.myhuaweiclouds.com:8000/postgres",
      "dbadmin", "XXX", "org.postgresql.Driver")


    val ssc = new StreamingContext(sparkConf, Seconds(duration.toInt))

    val params = Map(
      DISUtils.PROPERTY_ENDPOINT -> endpoint,
      DISUtils.PROPERTY_REGION_ID -> region,
      DISUtils.PROPERTY_AK -> ak,
      DISUtils.PROPERTY_SK -> sk,
      DISUtils.PROPERTY_PROJECT_ID -> projectId)
    val stream = DISUtils.createDirectStream[String, String](
      ssc,
      ConsumerStrategies.Assign[String, String](streamName, params, startingOffsets))

    val realValue = stream.map(record => record.value.toString)

    realValue.foreachRDD {
      rdd =>
          rdd.foreachPartition(savePartitionToDws(DwsInfo(dwsurl, username, dwspassword, driver)))
    }

    //start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def savePartitionToDws(dwsInfo: DwsInfo) (iter: Iterator[String]): Unit = {
    var connection: Connection = null
    Class.forName(dwsInfo.driver)
    // val property = new Property
    try {
      connection = DriverManager.getConnection(dwsInfo.url, dwsInfo.userName, dwsInfo.password)
      val statement = connection.createStatement()
      val sql = getBatchInsertSql(iter)
      statement.execute(sql)
      statement.close
    } finally {
      if (connection != null) connection.close()
    }
  }

  def getBatchInsertSql(iter: Iterator[String]): String = {
    val parser = new JsonParser
    val stringBuilder = new StringBuilder
    stringBuilder.append("insert into dbadmin.sample_tbl values ")
    var hasRecord = false
    while (iter.hasNext) {
      try {
        hasRecord = true
        val message = iter.next()
        val obj = parser.parse(message).getAsJsonObject
        val equId = obj.get("equId").getAsString
        val equType = obj.get("equType").getAsInt
        val zoneId = obj.get("zoneId").getAsInt
        val uploadTime = obj.get("uploadTime").getAsBigInteger
        val runningTime = obj.get("runningTime").getAsDouble
        val temperature = obj.get("temperature").getAsDouble
        val pressure = obj.get("pressure").getAsDouble
        val waterLine = obj.get("waterLine").getAsDouble
        val targetTemperature = obj.get("targetTemperature").getAsDouble
        val targetPressure = obj.get("targetPressure").getAsDouble
        val targetWaterLine = obj.get("targetWaterLine").getAsDouble
        val feedWater = obj.get("feedWater").getAsDouble
        val noxEmissions = obj.get("noxEmissions").getAsDouble
        val unitLoad = obj.get("unitLoad").getAsDouble
        val fieldSeq = Seq(equId, equType, zoneId, uploadTime, runningTime, temperature, pressure, waterLine,
          targetTemperature, targetPressure, targetWaterLine, feedWater, noxEmissions, unitLoad)
        stringBuilder.append(fieldSeq.mkString("(", ",", "),"))
      } catch {
        case e: Exception =>
          log.error("Parse message failed!", e)
      }
    }
    if (hasRecord) {
      stringBuilder.toString.stripSuffix(",") + ";"
    } else {
      ""
    }
  }
}
