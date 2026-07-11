/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.rdd.RDD

object RDDToDatasetMigrationNotSelfContained {
  // The RDDs come in as parameters (a non-convertible origin), so a rename like
  // intersection -> intersect can't be guaranteed to land on a Dataset. The rule
  // logs it for manual migration instead of producing code that won't compile.
  def inSource(a: RDD[Int], b: RDD[Int]): Unit = {
    val r = a.intersection(b).collect() // assert: RDDToDatasetMigration
  }
}
