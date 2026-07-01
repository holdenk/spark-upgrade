package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationTypedOrigin {
  // Explicit-type-argument origins (`parallelize[Int](...)`) must be converted too,
  // both in the main chain and as a `union` argument -- otherwise the argument would
  // stay an RDD and `Dataset.union(RDD)` would not compile. The type argument must be
  // PRESERVED: for an empty seq it is what pins T (dropping it infers Nothing).
  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val a = spark.createDataset[Int](Seq(1, 2, 3))
    val e = spark.createDataset[Int](Seq())
    val r = a.union(spark.createDataset[Int](Seq(4))).union(e).collect()
  }
}
