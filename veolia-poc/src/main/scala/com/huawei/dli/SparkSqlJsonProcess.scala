package com.huawei.dli

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dis.{ConsumerStrategies, DISUtils}

/**
  * Created by wangfei on 2018/8/21.
  */
object SparkSqlJsonProcess {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Json Example")
      .master("local[4]")
      .getOrCreate()

    val df = spark.read.json("file:///Users/wangfei/Downloads/pathologyresultmongoentity.json")

    df.show()

  }


}
