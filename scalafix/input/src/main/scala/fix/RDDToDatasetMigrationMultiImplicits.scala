/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationMultiImplicits {
  // More than one `implicits._` import makes the target SparkSession ambiguous when
  // an origin is not written as `<session>.sparkContext.<origin>`, so the file is
  // blocked rather than rewritten against a guessed session.
  def inSource(spark: SparkSession, other: SparkSession): Unit = {
    import spark.implicits._
    import other.implicits._
    val out = spark.sparkContext.parallelize(Seq(1, 2, 3)).map(_ + 1).collect() // assert: RDDToDatasetMigration
  }
}
