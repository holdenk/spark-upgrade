/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.Dataset

object RDDToDatasetMigrationNoEncoderNoImport {
  // A `.rdd`-drop chain with no map/flatMap/mapPartitions and no createDataset origin
  // needs no Encoder, so it must rewrite even with NO `implicits._` import in scope.
  def inSource(ds: Dataset[Int]): Long =
    ds.rdd.filter(_ > 1).count()
}
