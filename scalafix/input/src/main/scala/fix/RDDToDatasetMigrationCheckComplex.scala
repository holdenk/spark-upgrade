/*
rule = RDDToDatasetMigrationCheck
 */
package fix

import org.apache.spark.SparkContext

object RDDToDatasetMigrationCheckComplex {
  // reduceByKey and join are key/pair RDD functions with no straightforward
  // Dataset equivalent, so each is flagged as blocking the migration. The plain
  // map is simple and must NOT be flagged when the pipeline is already blocked.
  def inSource(sc: SparkContext): Unit = {
    val pairs = sc.parallelize(Seq((1, "a"), (2, "b"), (1, "c")))
    val reduced = pairs.reduceByKey(_ + _) // assert: RDDToDatasetMigrationCheck
    val joined = pairs.join(pairs) // assert: RDDToDatasetMigrationCheck
    val mapped = pairs.map(p => p._1)
  }
}
