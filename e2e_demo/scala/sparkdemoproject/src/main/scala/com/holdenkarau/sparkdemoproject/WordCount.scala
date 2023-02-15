package com.holdenkarau.sparkDemoProject

/**
 * Everyone's favourite wordcount example.
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WordCount {
  /**
   * A slightly more complex than normal wordcount example with optional
   * separators and stopWords. Splits on the provided separators, removes
   * the stopwords, and converts everything to lower case.
   */
  def dataFrameWC(df : DataFrame,
    separators : Array[Char] = " ".toCharArray,
    stopWords : Set[String] = Set("the")): DataFrame = {
    // Yes this is deprecated, but it should get rewritten by our magic.
    val spark  = SQLContext.getOrCreate(SparkContext.getOrCreate())
    import spark.implicits._
    val splitPattern = "[" + separators.mkString("") + "]"
    val stopArray = array(stopWords.map(lit).toSeq:_*)
    val words = df.select(explode(split(lower(col("value")), splitPattern)).as("words")).filter(
      not(array_contains(stopArray, col("words"))))
    // This will need to be re-written in 2 -> 3
    // Normally we would use groupBy(col("words")) instead by that doesn't require the migration step :p
    def keyMe(x: Row): String = {
      x.apply(0).asInstanceOf[String]
    }
    words.groupByKey(keyMe).count().select(col("value").as("word"), col("count(1)").as("count")).orderBy("count")
  }

  def withStopWordsFiltered(rdd : RDD[String],
    separators : Array[Char] = " ".toCharArray,
    stopWords : Set[String] = Set("the")): RDD[(String, Long)] = {
    val spark  = SQLContext.getOrCreate(SparkContext.getOrCreate())
    import spark.implicits._
    val df = rdd.toDF
    val resultDF = dataFrameWC(df, separators, stopWords)
    resultDF.rdd.map { row =>
      (row.apply(0).asInstanceOf[String], row.apply(1).asInstanceOf[Long])
    }
  }

}
