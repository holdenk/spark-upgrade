/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationSimple {
  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val out = spark.sparkContext.parallelize(Seq(1, 2, 3)).map(_ + 1).filter(_ % 2 == 0).collect()
  }
}
