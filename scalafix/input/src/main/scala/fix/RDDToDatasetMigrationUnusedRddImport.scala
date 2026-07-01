/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationUnusedRddImport {
  // A stale/unused `import org.apache.spark.rdd.RDD` (or a comment/string mentioning
  // the FQCN) must NOT block a safe rewrite: there is no actual RDD[...] type that
  // would dangle, so the pipeline still migrates.
  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val out = spark.sparkContext.parallelize(Seq(1, 2, 3)).map(_ + 1).collect()
  }
}
