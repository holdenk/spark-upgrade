/*
rule = RDDToDatasetMigrationCheck
 */
package fix

import org.apache.spark.{Partitioner, SparkContext}

/** A custom partitioner has no Dataset equivalent at all. */
class HashModPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = (key.hashCode & Int.MaxValue) % partitions
}

object RDDToDatasetMigrationCheckUnsupported {
  // This pipeline relies on RDD operations that have no Dataset equivalent
  // (custom partitioning, zipWithIndex, partition-index aware mapping, an
  // arbitrary RDD-only action, and an RDD sink), so the rule must report it as
  // not migratable. The plain map is simple and must NOT be flagged.
  def inSource(sc: SparkContext): Unit = {
    val pairs = sc.parallelize(Seq((1, "a"), (2, "b"), (3, "c")))
    val partitioned = pairs.partitionBy(new HashModPartitioner(4)) // assert: RDDToDatasetMigrationCheck
    val indexed = partitioned.zipWithIndex() // assert: RDDToDatasetMigrationCheck
    val tagged = indexed.mapPartitionsWithIndex { (idx, it) => // assert: RDDToDatasetMigrationCheck
      it.map { case (kv, i) => (idx, kv, i) }
    }
    val highest = pairs.top(2) // assert: RDDToDatasetMigrationCheck
    tagged.map(_.toString).saveAsTextFile("/tmp/out") // assert: RDDToDatasetMigrationCheck
  }
}
