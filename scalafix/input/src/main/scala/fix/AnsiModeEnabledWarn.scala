/*
rule=AnsiModeEnabledWarn
 */
package fix

import org.apache.spark.sql.SparkSession

object AnsiModeEnabledWarn {
  def inSource(): Unit = {
    val spark = SparkSession.builder // assert: AnsiModeEnabledWarn
      .appName("example")
      .getOrCreate()
    spark.stop()
  }
}
