package com.holdenkarau

import java.io.File

import com.holdenkarau.spark.testing.ScalaDataFrameSuiteBase
import com.holdenkarau.spark.testing.Utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WAPIcebergSpec extends AnyFunSuite with ScalaDataFrameSuiteBase with Matchers {

  override protected def enableHiveSupport = false
  override protected def enableIcebergSupport = true

  // For now https://github.com/sbt/sbt/issues/7137 no test.
  /**
   * Ok testing this does not work with sbt for reasons that I'm a bit fuzzy on but it looks like there has been some classpath
   * shenanigins as of late.
   *
  test("WAPIcebergSpec should be called on iceberg commit") {
    val cl = Thread.currentThread.getContextClassLoader
    cl.loadClass("org.apache.iceberg.spark.SparkSessionCatalog")
    spark.sql("CREATE TABLE local.db.table (id bigint, data string) USING iceberg")
    WAPIcebergListener.lastLog shouldEqual "farts"
  }
   */
}
