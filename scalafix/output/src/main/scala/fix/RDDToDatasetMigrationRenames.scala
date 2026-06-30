package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationRenames {
  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val a = spark.createDataset(Seq(1, 2, 3))
    val b = spark.createDataset(Seq(2, 3, 4))
    // intersection -> intersect (rename); union stays union, but its operand must
    // also trace to a convertible origin (the inline parallelize does).
    val r = a.intersect(b).union(spark.createDataset(Seq(3))).collect()
  }
}
