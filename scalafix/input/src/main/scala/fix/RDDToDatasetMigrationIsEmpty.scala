/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationIsEmpty {
  // RDD.isEmpty() is declared with parens; Dataset.isEmpty is parameterless, so
  // `dataset.isEmpty()` would not compile. isEmpty is blocked.
  def inSource(spark: SparkSession): Unit = {
    val empty = spark.sparkContext.parallelize(Seq(1, 2, 3)).filter(_ > 10).isEmpty() // assert: RDDToDatasetMigration
  }
}
