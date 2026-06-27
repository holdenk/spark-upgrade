/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationInfixRename {
  // Infix-spelled rename op: the rename must still be applied (otherwise the
  // origins would be converted to Datasets but `intersection` left in place).
  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val a = spark.sparkContext.parallelize(Seq(1, 2, 3))
    val b = spark.sparkContext.parallelize(Seq(2, 3, 4))
    val r = (a intersection b).collect()
  }
}
