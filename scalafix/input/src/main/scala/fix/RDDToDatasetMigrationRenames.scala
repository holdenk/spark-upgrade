/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationRenames {
  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val a = spark.sparkContext.parallelize(Seq(1, 2, 3))
    val b = spark.sparkContext.parallelize(Seq(2, 3, 4))
    val r = a.intersection(b).subtract(spark.sparkContext.parallelize(Seq(3))).collect()
  }
}
