/*
rule = RDDToDatasetMigrationCheck
 */
package fix

import org.apache.spark.SparkContext

object RDDToDatasetMigrationCheckSimple {
  // Every RDD operation here (map / filter / distinct / count) has a direct
  // Dataset equivalent, so the rule should report that this usage can be
  // migrated to the Dataset API.
  def inSource(sc: SparkContext): Unit = {
    val base = sc.parallelize(Seq(1, 2, 3, 4))
    val result = base.map(_ + 1).filter(_ % 2 == 0).distinct().count() // assert: RDDToDatasetMigrationCheck
  }
}
