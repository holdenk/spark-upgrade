/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationNoImplicits {
  // Fully migratable, but the Dataset encoders import is missing: the rule logs
  // that it is needed instead of rewriting (which would not compile without it).
  def inSource(spark: SparkSession): Unit = {
    val out = spark.sparkContext.parallelize(Seq(1, 2, 3)).map(_ + 1).collect() // assert: RDDToDatasetMigration
  }
}
