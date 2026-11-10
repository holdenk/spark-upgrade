/*
rule=RemovedSparkConfWarn
 */
package fix

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RemovedSparkConfWarn {
  def inSource(spark: SparkSession): Unit = {
    val conf = new SparkConf()
      .set("spark.blacklist.enabled", "true") // assert: RemovedSparkConfWarn
      .set("spark.blacklist.killBlacklistedExecutors", "true") // assert: RemovedSparkConfWarn
      .set("spark.scheduler.blacklist.unschedulableTaskSetTimeout", "120s") // assert: RemovedSparkConfWarn
      .set("spark.yarn.blacklist.executor.launch.blacklisting.enabled", "true") // assert: RemovedSparkConfWarn
      .set("spark.scheduler.executorTaskBlacklistTime", "1000") // assert: RemovedSparkConfWarn
      .set("spark.shuffle.unsafe.file.output.buffer", "64k") // assert: RemovedSparkConfWarn
      .set("spark.yarn.max.executor.failures", "10") // assert: RemovedSparkConfWarn
      .set("spark.yarn.executor.failuresValidityInterval", "1h") // assert: RemovedSparkConfWarn
      .set("spark.launcher.childConectionTimeout", "10000") // assert: RemovedSparkConfWarn
      .set("spark.executor.plugins", "com.example.Plugin") // assert: RemovedSparkConfWarn
      // Fuzzy fallback: not in the exact table but shaped like a blacklist key.
      .set("spark.custom.blacklist.nodes", "a,b") // assert: RemovedSparkConfWarn
      // Already migrated or unrelated keys: not flagged.
      .set("spark.excludeOnFailure.enabled", "true")
      .set("spark.executor.maxNumFailures", "10")
      .set("spark.plugins", "com.example.Plugin")
      .set("cache.blacklist.ttl", "60")
    spark.conf.set("spark.sql.hive.verifyPartitionPath", "true") // assert: RemovedSparkConfWarn
    // Fragments of a key are skipped; an interpolation without arguments is the whole key.
    val x = "nodes"
    val fragments = new SparkConf()
      .set(s"spark.blacklist.$x", "a")
      .set("spark.blacklist." + x, "a")
      .set("spark.custom.blacklist.", "a")
      .set(s"spark.blacklist.enabled", "true") // assert: RemovedSparkConfWarn
  }
}
