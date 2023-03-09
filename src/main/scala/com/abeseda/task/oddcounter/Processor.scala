package com.abeseda.task.oddcounter

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

/**
 * Processor does read, computation and write for the specified task.
 */
class Processor {

  private val COLUMN_KEY   = "key"
  private val COLUMN_VALUE = "value"
  private val COLUMN_COUNT = "count"

  /**
   * Processes input path, does the computations and saves data to the output path
   * @param sparkSession spark session instance
   * @param inputPath input path to data
   * @param outputPath output path where data be written
   */
  def process(sparkSession: SparkSession, inputPath: String, outputPath: String): Unit = {
    val inputDataset = readInputData(sparkSession, inputPath)

    val processedDataset = doLogic(inputDataset)

    writeOutputData(processedDataset, outputPath)
  }

  protected[oddcounter] def readInputData(sparkSession: SparkSession, inputPath: String): Dataset[Record] = {
    // using wholetext=true to put each file in separate row in order to remove header
    val inputDf = sparkSession.read
      .option("wholetext", "true")
      .textFile(inputPath)
      .repartition(sparkSession.sessionState.conf.numShufflePartitions)   // in order to utilize all cores
    inputDf.mapPartitions {
      _.flatMap(row => {
        val allLines = scala.io.Source.fromString(row).getLines.toList
        // get all lines except header
        val lines = allLines.tail
        // determine separator
        val separatorOpt = lines.headOption.map(h => if (h.contains(",")) "," else "\t")
        // process all lines from the file
        separatorOpt
          .map(separator => {
            lines.map(line => {
              val values = line.split(separator, -1)
              // in case there is no second value due to \t usage -> add it
              val allValues = if (values.length == 2) values else values ++ Seq.fill(2 - values.length)("")
              // cast to int values
              val intValues = allValues.map(v => if (StringUtils.isBlank(v)) 0 else v.toInt)
              Record(intValues(0), intValues(1))
            })
          })
          .getOrElse(Nil)
      })
    }(Encoders.product[Record])
  }

  protected[oddcounter] def doLogic(dataset: Dataset[Record]): Dataset[Record] = {
    dataset
      .groupBy(COLUMN_KEY, COLUMN_VALUE)
      .count()
      .where(col(COLUMN_COUNT).mod(2) =!= 0)
      .selectExpr(COLUMN_KEY, s"CAST($COLUMN_COUNT AS INT) AS $COLUMN_VALUE")
      .as[Record](Encoders.product[Record])
  }

  protected[oddcounter] def writeOutputData(dataset: Dataset[Record], outputPath: String): Unit = {
    dataset.write
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(outputPath)
  }

}
