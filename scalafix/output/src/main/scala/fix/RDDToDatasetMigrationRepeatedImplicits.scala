package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationRepeatedImplicits {
  // The SAME `import spark.implicits._` appears in two methods -- the normal pattern
  // once the encoders import must be in scope per method. That is ONE session, not an
  // ambiguous one, and must not block the rewrite.
  def prepare(spark: SparkSession): Long = {
    import spark.implicits._
    spark.createDataset(Seq(0)).count()
  }

  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val out = spark.createDataset(Seq(1, 2, 3)).map(_ + 1).collect()
  }
}
