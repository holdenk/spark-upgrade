/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationBlocked {
  def inSource(spark: SparkSession): Unit = {
    val pairs = spark.sparkContext.parallelize(Seq((1, "a"), (2, "b")))
    val counts = pairs.reduceByKey(_ + _) // assert: RDDToDatasetMigration
  }
}
