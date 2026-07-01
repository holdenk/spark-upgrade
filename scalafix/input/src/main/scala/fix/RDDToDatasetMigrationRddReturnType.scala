/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationRddReturnType {
  // The chain is migratable, but the declared return type is RDD[Int]; rewriting the
  // body to a Dataset would leave the annotation dangling, so the file is blocked.
  def inSource(spark: SparkSession): RDD[Int] =
    spark.sparkContext.parallelize(Seq(1, 2, 3)).map(_ + 1) // assert: RDDToDatasetMigration
}
