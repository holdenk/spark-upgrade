/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationParallelizeArgs {
  // parallelize(seq, numSlices) is blocked: createDataset(seq).repartition(n) would
  // force a shuffle that reorders rows, so it is not a faithful rewrite.
  def inSource(spark: SparkSession): Unit = {
    val out = spark.sparkContext.parallelize(Seq(1, 2, 3), 4).map(_ + 1).collect() // assert: RDDToDatasetMigration
  }
}
