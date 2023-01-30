package com.holdenkarau.spark.upgrade.wap.plugin

import java.io.File

import com.holdenkarau.spark.testing.ScalaDataFrameSuiteBase
import com.holdenkarau.spark.testing.Utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkConf

class WAPIcebergSpec extends AnyFunSuite with ScalaDataFrameSuiteBase with Matchers {

  override protected def enableHiveSupport = false
  override protected def enableIcebergSupport = true

  override def conf: SparkConf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost").
      // Hack
      set("spark.driver.userClassPathFirst", "true").
      set("spark.executor.userClassPathFirst", "true")
  }

  test("WAPIcebergSpec should be called on iceberg commit") {
    val cl = Thread.currentThread.getContextClassLoader
    cl.loadClass("org.apache.iceberg.spark.SparkSessionCatalog")
    spark.sql("CREATE TABLE local.db.table (id bigint, data string) USING iceberg")
    WAPIcebergListener.lastLog shouldEqual "farts"
  }
}
