/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationTypeArgArity {
  // An op written with an explicit type argument parses as Term.Apply(Term.ApplyType(...)),
  // which must still be collected WITH its args so the two-arg form is blocked (Dataset
  // .mapPartitions has no preservesPartitioning overload), not silently rewritten.
  def inSource(spark: SparkSession): Unit = {
    val out = spark.sparkContext
      .parallelize(Seq(1, 2, 3))
      .mapPartitions[Int](it => it, true) // assert: RDDToDatasetMigration
      .collect()
  }
}
