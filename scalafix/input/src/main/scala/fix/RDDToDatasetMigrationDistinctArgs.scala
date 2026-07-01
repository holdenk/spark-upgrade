/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationDistinctArgs {
  // RDD.distinct(numPartitions) has no Dataset equivalent (Dataset.distinct takes no
  // args), so the one-arg form is blocked. (distinct() with no args is rewritten.)
  def inSource(spark: SparkSession): Unit = {
    val out = spark.sparkContext.parallelize(Seq(1, 2, 2, 3)).distinct(2).collect() // assert: RDDToDatasetMigration
  }
}
