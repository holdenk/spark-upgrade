/*
rule = RDDToDatasetMigrationCheck
 */
package fix

import org.apache.spark.SparkContext

object RDDToDatasetMigrationCheckNotLikeForLike {
  // subtract has a same-name Dataset method but NOT the same semantics (RDD
  // removes every row whose value is in the other RDD -- a left-anti join), so
  // the checker must report it as blocking, not advertise the pipeline as
  // migratable. Keeps the checker aligned with the rewrite rule's manualReasons.
  def inSource(sc: SparkContext): Unit = {
    val a = sc.parallelize(Seq(1, 1, 2))
    val b = sc.parallelize(Seq(1))
    val r = a.subtract(b).collect() // assert: RDDToDatasetMigrationCheck
  }
}
