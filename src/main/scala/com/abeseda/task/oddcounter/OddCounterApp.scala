package com.abeseda.task.oddcounter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object OddCounterApp {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.out.println("Usage: odd-counter-app <input_path> <output_path> <aws_profile>")
      System.exit(-1)
    }

    val (inputPath, outputPath, awsProfile) = (args(0), args(1), args(2))

    // load configs and init spark
    val sparkConfig  = getSparkConf
    val sparkSession = sparkInit(sparkConfig)

    // do the processing
    val processor = new Processor()
    processor.process(sparkSession, inputPath, outputPath)
  }

  private def getSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.hadoop.fs.defaultFS", "file:///")
    sparkConf.set("spark.sql.session.timeZone", "UTC")
    sparkConf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    sparkConf
  }

  private def sparkInit(sparkConf: SparkConf): SparkSession = {
    SparkSession.builder.appName("OddCounter").config(sparkConf).getOrCreate()
  }

}
