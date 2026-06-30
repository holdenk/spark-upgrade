/*
rule = RDDToDatasetMigration
 */
package fix

import org.apache.spark.sql.SparkSession

object RDDToDatasetMigrationTextFileArgs {
  // textFile(path, minPartitions) is blocked: DataFrameReader.textFile has no
  // minPartitions argument, so the hint would be silently dropped.
  def inSource(spark: SparkSession): Unit = {
    val lines = spark.sparkContext.textFile("/data/in.txt", 8).map(_.length).collect() // assert: RDDToDatasetMigration
  }
}
