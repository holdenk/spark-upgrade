/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationSubtract {
  // RDD.subtract removes every row whose value is present in the other RDD (key
  // removal); neither Dataset.except (EXCEPT DISTINCT) nor exceptAll (EXCEPT ALL)
  // reproduces it, so subtract is blocked rather than renamed.
  def inSource(spark: SparkSession): Unit = {
    val a = spark.sparkContext.parallelize(Seq(1, 1, 2))
    val r = a.subtract(spark.sparkContext.parallelize(Seq(1))).collect() // assert: RDDToDatasetMigration
  }
}
