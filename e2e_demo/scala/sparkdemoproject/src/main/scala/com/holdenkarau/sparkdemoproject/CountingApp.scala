package com.holdenkarau.sparkDemoProject

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object CountingLocalApp {
  def main(args: Array[String]) = {
    val (inputFile, outputFile) = (args(0), args(1))
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("my awesome app")

    Runner.run(conf, inputFile, outputFile)
  }
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp {
  def main(args: Array[String]) = {
    val (inputFile, outputFile) = (args(0), args(1))

    // spark-submit command should supply all necessary config elements
    Runner.run(new SparkConf(), inputFile, outputFile)
  }
}

object Runner {
  def run(conf: SparkConf, inputPath: String, outputTable: String): Unit = {
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.format("text").load(inputPath)
    val counts = WordCount.dataFrameWC(df)
    counts.cache()
    counts.count()
    // Try and append, or create.
    try {
      counts.write.format("iceberg").mode("overwrite").save(outputTable)
    } catch {
      case e =>
        spark.sql(s"CREATE TABLE ${outputTable} (word string, count long) USING iceberg")
        counts.write.format("iceberg").saveAsTable(outputTable)
        spark.sql(s"ALTER TABLE ${outputTable} SET write.wap.enabled=true")
    }
  }
}
