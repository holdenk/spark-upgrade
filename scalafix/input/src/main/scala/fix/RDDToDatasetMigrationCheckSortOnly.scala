/*
rule = RDDToDatasetMigrationCheck
 */
package fix

import org.apache.spark.SparkContext

object RDDToDatasetMigrationCheckSortOnly {
  // sortBy maps to Dataset.orderBy / sort, so an RDD pipeline whose only
  // operation is a sort is simple enough to migrate to the Dataset API.
  def inSource(sc: SparkContext): Unit = {
    val sorted = sc.parallelize(Seq(3, 1, 2)).sortBy(x => x) // assert: RDDToDatasetMigrationCheck
  }
}
