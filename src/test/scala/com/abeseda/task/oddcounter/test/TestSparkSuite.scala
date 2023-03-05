package com.abeseda.task.oddcounter.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

trait TestSparkSuite extends Suite with Matchers with Logging {

  protected lazy val spark: SparkSession = TestSparkSessionHolder.testSparkSession

  protected def createDataFrame(spark: SparkSession, schema: StructType, rows: Seq[Row]): DataFrame = {
    spark.createDataFrame(rows.asJava, schema)
  }
}

object TestSparkSessionHolder extends TestSparkContext {

  protected[test] lazy val testSparkSession: SparkSession = {
    createSparkSession()
  }

}
