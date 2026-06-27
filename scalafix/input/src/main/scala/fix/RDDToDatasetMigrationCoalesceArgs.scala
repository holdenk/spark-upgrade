/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationCoalesceArgs {
  // `coalesce(n, shuffle)` has no Dataset equivalent (Dataset.coalesce takes a
  // single Int), so the two-arg form is blocked rather than rewritten to code
  // that would not compile.
  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val out = spark.sparkContext.parallelize(Seq(1, 2, 3)).coalesce(2, true).collect() // assert: RDDToDatasetMigration
  }
}
