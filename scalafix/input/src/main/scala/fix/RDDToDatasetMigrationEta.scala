/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationEta {
  // An RDD op used as an eta-expanded method value (`foreach _`) is blocked: leaving
  // the eta form on a Dataset receiver changes/ambiguates which method it refers to.
  def inSource(spark: SparkSession): Unit = {
    val f = spark.sparkContext.parallelize(Seq(1, 2, 3)).foreach _ // assert: RDDToDatasetMigration
    f((_: Int) => ())
  }
}
