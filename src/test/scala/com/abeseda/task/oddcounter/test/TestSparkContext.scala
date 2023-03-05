package com.abeseda.task.oddcounter.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.TimeZone

trait TestSparkContext {

  protected def createSparkSession(): SparkSession = {
    val conf = new SparkConf().setMaster("local").setAppName("Spark spark.test")
    conf.set("spark.sql.shuffle.partitions", "1") // set to low value to speed up tests

    SparkSession.builder().config(applyCommonSettings(conf)).getOrCreate()
  }

  private def applyCommonSettings(conf: SparkConf): SparkConf = {
    java.util.TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
    conf.set("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    conf
  }

  protected def withSparkSession(testMethod: SparkSession => Any): Unit = {
    val sparkSession = createSparkSession()
    try {
      testMethod(sparkSession)
    } finally {
      sparkSession.stop()
    }
  }

}
