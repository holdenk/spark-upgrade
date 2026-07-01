/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationImplicitsScope {
  // The implicits import lives in ANOTHER method, so the encoders are NOT in scope
  // where this chain would be rewritten; a file-wide import check would wrongly
  // rewrite it into code that does not compile. The rule must log instead.
  def prepare(spark: SparkSession): Unit = {
    import spark.implicits._
    val warm = spark.createDataset(Seq(0))
  }

  def inSource(spark: SparkSession): Unit = {
    val out = spark.sparkContext.parallelize(Seq(1, 2, 3)).map(_ + 1).collect() // assert: RDDToDatasetMigration
  }
}
