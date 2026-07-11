/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.{Dataset, SparkSession}

object RDDToDatasetMigrationRddDrop {
  def inSource(spark: SparkSession, ds: Dataset[String]): Unit = {
    import spark.implicits._
    val lengths = ds.rdd.map(s => s.length).collect()
  }
}
