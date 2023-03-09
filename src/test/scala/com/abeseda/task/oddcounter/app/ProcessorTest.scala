package com.abeseda.task.oddcounter.app

import java.nio.file.{Files, Path}
import java.util.UUID

import com.abeseda.task.oddcounter.test.TestSparkSuite
import com.abeseda.task.oddcounter.{Processor, Record}
import org.apache.spark.sql.Encoders
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.io.Source
import scala.util.Try

class ProcessorTest extends AnyWordSpec with TestSparkSuite with MockitoSugar {

  "Processor" when {

    "readInputData" should {

      "return input dataset when no data" in {
        val path = getClass.getClassLoader.getResource("data/sample01").getPath

        val result = new Processor().readInputData(spark, path)

        val resultRecords = result.collect().toSeq
        resultRecords should equal(Nil)
      }

      "return input dataset with mixed csv and tsv data" in {
        val path = getClass.getClassLoader.getResource("data/sample02").getPath

        val result = new Processor().readInputData(spark, path)

        val resultRecords = result.collect().toSeq
        resultRecords should equal(
          Seq(
            Record(0, 4),
            Record(0, 4),
            Record(4, 1),
            Record(4, 1),
            Record(4, 1),
            Record(5, 2),
            Record(5, 2),
            Record(3, 0),
            Record(3, 0),
            Record(10, 12),
            Record(10, 12),
            Record(10, 0),
            Record(20, 21),
            Record(20, 21)
          )
        )
      }

      "return input dataset with csv data only" in {
        val path = getClass.getClassLoader.getResource("data/sample03").getPath

        val result = new Processor().readInputData(spark, path)

        val resultRecords = result.collect().toSeq
        resultRecords should equal(
          Seq(
            Record(1, 2),
            Record(0, 3),
            Record(4, 0),
            Record(0, 0)
          )
        )
      }

      "return input dataset with tsv data only" in {
        val path = getClass.getClassLoader.getResource("data/sample04").getPath

        val result = new Processor().readInputData(spark, path)

        val resultRecords = result.collect().toSeq
        resultRecords should equal(
          Seq(
            Record(0, 10),
            Record(12, 0),
            Record(11, 12),
            Record(0, 0)
          )
        )
      }

    }

    "doLogic" should {

      "return processed data on empty input" in {
        val inputDataset = spark.createDataset(Nil)(Encoders.product[Record])

        val result = new Processor().doLogic(inputDataset)

        val resultRecords = result.collect().toSeq
        resultRecords should equal(Nil)
      }

      "return processed data when no odd counts in input" in {
        val inputDataset = spark.createDataset(
          Seq(
            Record(0, 4),
            Record(0, 4),
          )
        )(Encoders.product[Record])

        val result = new Processor().doLogic(inputDataset)

        val resultRecords = result.collect().toSeq
        resultRecords should equal(Nil)
      }

      "return processed data when odd counts in input" in {
        val inputDataset = spark.createDataset(
          Seq(
            Record(0, 4),
            Record(0, 4),
            Record(4, 1),
            Record(4, 1),
            Record(4, 2),
            Record(4, 2),
            Record(4, 1),
            Record(5, 2),
            Record(5, 2),
            Record(10, 0),
            Record(20, 21),
            Record(20, 21)
          )
        )(Encoders.product[Record])

        val result = new Processor().doLogic(inputDataset)

        val resultRecords = result.collect().toSeq
        resultRecords should equal(
          Seq(
            Record(4, 3),
            Record(10, 1)
          )
        )
      }

    }

    "writeOutputData" should {

      "write empty output data" in {
        val outputPath = s"/tmp/${UUID.randomUUID().toString}"
        val dataset = spark.createDataset(Nil)(Encoders.product[Record])

        new Processor().writeOutputData(dataset, outputPath)

        val files = Files.list(Path.of(outputPath)).iterator().asScala.toSeq
        val result = files.filter(p => p.getFileName.toString.endsWith(".csv")).map(readFile).mkString("\n")
        System.out.println(result)
        result should equal("")
      }

      "write output data in tsv format" in {
        val outputPath = s"/tmp/${UUID.randomUUID().toString}"
        val dataset = spark.createDataset(
          Seq(
            Record(4, 3),
            Record(10, 1)
          )
        )(Encoders.product[Record])

        new Processor().writeOutputData(dataset, outputPath)

        val files = Files.list(Path.of(outputPath)).iterator().asScala.toSeq
        val result = files.filter(p => p.getFileName.toString.endsWith(".csv")).map(readFile).mkString("\n")
        System.out.println(result)
        result should equal("4\t3\n10\t1\n")
      }

    }

  }

  private def readFile(path: Path): String = {
    Try(Source.fromFile(path.toFile))
      .map(_.mkString).getOrElse(throw new RuntimeException(s"Failed to read path: $path"))
  }

}
