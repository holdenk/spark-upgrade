/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationShadowedVal {
  def makeRdd(spark: SparkSession) = spark.sparkContext.emptyRDD[Int]

  // Two methods bind the SAME val name `xs`. Operand tracing must resolve the
  // reference by semantic symbol, not by name: `bad`'s `xs` comes from a helper
  // (not a convertible origin), so its union must be blocked even though a
  // different method's `xs` IS a convertible origin.
  def good(spark: SparkSession): Unit = {
    import spark.implicits._
    val xs = spark.sparkContext.parallelize(Seq(1, 2, 3))
    val r = xs.union(spark.sparkContext.parallelize(Seq(4))).collect()
  }

  def bad(spark: SparkSession): Unit = {
    import spark.implicits._
    val xs = makeRdd(spark)
    val r = xs.union(spark.sparkContext.parallelize(Seq(5))).collect() // assert: RDDToDatasetMigration
  }
}
