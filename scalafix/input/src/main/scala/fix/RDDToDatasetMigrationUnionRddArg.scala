/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationUnionRddArg {
  // `union` is binary: its argument must also become a Dataset. Here `other` is an
  // RDD that does not trace to a convertible origin, so the union is blocked rather
  // than rewritten into `Dataset.union(<RDD>)`, which would not compile.
  def inSource(spark: SparkSession, other: RDD[Int]): Unit = {
    val r = spark.sparkContext.parallelize(Seq(1, 2, 3)).union(other).collect() // assert: RDDToDatasetMigration
  }
}
