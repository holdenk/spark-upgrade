/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationSample {
  // RDD.sample and Dataset.sample use different samplers and seed derivation, so
  // the same seed yields different rows. sample is blocked to avoid a silent change.
  def inSource(spark: SparkSession): Unit = {
    val s = spark.sparkContext.parallelize(Seq(1, 2, 3, 4)).sample(false, 0.3, 42L).collect() // assert: RDDToDatasetMigration
  }
}
