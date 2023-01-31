package com.holdenkarau.spark.upgrade.wap.plugin

import java.io.File

import scala.util.matching.Regex

import com.holdenkarau.spark.testing.ScalaDataFrameSuiteBase
import com.holdenkarau.spark.testing.Utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkConf

/**
 * This tests both the listener and the agent since if the agent is not loaded the
 * WAPIcebergListener will not be registered and none of the tests will pass :D
 */
class WAPIcebergSpec extends AnyFunSuite with ScalaDataFrameSuiteBase with Matchers {

  val delay = 2

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
    val re = """IcebergListener: Created snapshot (\d+) on table (.+?) summary .*? from operation (.+)""".r
    spark.sql("CREATE TABLE local.db.table (id bigint, data string) USING iceberg")
    // there _might be_ a timing race condition here since were using a listener
    // that is not blocking the write path.
    spark.sql("INSERT INTO local.db.table VALUEs (1, 'timbit')")
    Thread.sleep(delay)
    val firstLog = WAPIcebergListener.lastLog
    firstLog should fullyMatch regex re
    firstLog match {
      case re(snapshot, table, op) =>
        snapshot.toLong should be > 0L
        table should be ("local.db.table")
        op should be ("append")
    }
    spark.sql("INSERT INTO local.db.table VALUEs (2, 'timbot')")
    Thread.sleep(delay)
    val secondLog = WAPIcebergListener.lastLog
    secondLog should not equal firstLog
    secondLog should fullyMatch regex re
  }
}
