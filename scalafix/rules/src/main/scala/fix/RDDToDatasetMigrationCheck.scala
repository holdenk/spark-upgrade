package fix

import scalafix.v1._
import scala.meta._

/**
 * Reported when an RDD operation is used that does not have a straightforward
 * Dataset/DataFrame equivalent, so the surrounding RDD pipeline can not be
 * migrated to the Dataset API automatically.
 */
case class RDDMigrationBlocked(tn: Tree, op: String, reason: String)
    extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    s"RDD operation '$op' blocks an automatic migration to the Dataset/DataFrame " +
      s"API ($reason). This RDD usage is not simple enough to migrate " +
      "automatically; migrate it by hand or leave it as an RDD."
}

/**
 * Reported once per file when *all* of the RDD operations used have a direct
 * Dataset/DataFrame equivalent, i.e. the RDD usage is simple enough that it
 * could all be migrated to the Dataset API.
 */
case class RDDMigrationPossible(tn: Tree, ops: Seq[String]) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    s"All RDD operations used here (${ops.mkString(", ")}) have direct " +
      "Dataset/DataFrame equivalents, so this RDD usage is simple enough to " +
      "migrate to the Dataset API (e.g. start from a Dataset/DataFrame or call " +
      ".toDS()/.toDF())."
}

/**
 * A simplistic (and optional) RDD -> Dataset migration check.
 *
 * Rather than rewriting the code, this rule inspects every RDD operation in a
 * file and decides whether the RDD usage is "sufficiently simple" that it could
 * all be migrated to the typed Dataset API:
 *
 *   - If every RDD operation has a direct Dataset/DataFrame equivalent it emits
 *     a single [[RDDMigrationPossible]] lint, flagging the pipeline as a good
 *     candidate for migration.
 *   - If any RDD operation has no simple equivalent (key/pair functions, joins,
 *     zips, custom partitioning, manual aggregations, ...) it emits a
 *     [[RDDMigrationBlocked]] lint at each such operation explaining why the
 *     pipeline can not be migrated automatically.
 *
 * Operations are recognised by their resolved symbol owner, so chained calls
 * (e.g. `rdd.map(...).filter(...)`) and the implicitly-converted pair RDD
 * functions are all detected. To stay conservative the rule treats any RDD API
 * it does not explicitly know about as blocking migration.
 */
class RDDToDatasetMigrationCheck
    extends SemanticRule("RDDToDatasetMigrationCheck") {

  override val description: String =
    "Checks whether the RDD usage in a file is simple enough to migrate to the " +
      "Dataset API."

  // Symbol owners whose members we treat as RDD operations. PairRDDFunctions and
  // friends are reached via implicit conversions but semanticdb still resolves
  // the call to the defining class, so a simple prefix match is enough.
  private val rddOwnerPrefixes: List[String] = List(
    "org/apache/spark/rdd/RDD#",
    "org/apache/spark/rdd/PairRDDFunctions#",
    "org/apache/spark/rdd/OrderedRDDFunctions#",
    "org/apache/spark/rdd/DoubleRDDFunctions#",
    "org/apache/spark/rdd/SequenceFileRDDFunctions#",
    "org/apache/spark/rdd/AsyncRDDActions#"
  )

  // RDD operations with a direct, like-for-like Dataset/DataFrame equivalent.
  // Kept aligned with the rewrite rule (RDDToDatasetMigration): ops whose Dataset
  // namesake differs in signature or runtime semantics (subtract, sample,
  // checkpoint, toLocalIterator, isEmpty, ++) are NOT here -- they carry tailored
  // entries in blockingReasons below instead. sortBy stays: orderBy/sort with a
  // column expression is a well-understood manual migration with no semantic trap.
  private val simpleOps: Set[String] = Set(
    "map", "flatMap", "filter", "mapPartitions",
    "foreach", "foreachPartition",
    "distinct", "union", "intersection",
    "count", "collect", "take", "first", "reduce",
    "cache", "persist", "unpersist",
    "coalesce", "repartition", "sortBy"
  )

  // RDD members that neither help nor hinder a migration (accessors / metadata).
  // They are ignored entirely so they do not block an otherwise simple pipeline.
  private val neutralOps: Set[String] = Set(
    "sparkContext", "context", "id", "name", "setName", "partitions",
    "getNumPartitions", "partitioner", "dependencies", "toDebugString",
    "getStorageLevel", "preferredLocations"
  )

  // Known-complex operations, with a hint about how to migrate them. Anything
  // that is neither simple, neutral, nor listed here is treated as blocking with
  // a generic message.
  private val blockingReasons: Map[String, String] = Map(
    // Same-name Dataset methods that are NOT like-for-like (kept in sync with the
    // rewrite rule's manualReasons -- a naive swap would not compile or would
    // silently change results):
    "subtract" -> "RDD.subtract removes every row whose value appears in the other RDD (key removal); neither except nor exceptAll matches -- use a left-anti join",
    "sample" -> "Dataset.sample uses a different sampler and seed derivation, so the same seed yields different rows",
    "checkpoint" -> "Dataset.checkpoint returns a new checkpointed Dataset instead of mutating in place",
    "toLocalIterator" -> "Dataset.toLocalIterator returns a java.util.Iterator, not a scala Iterator; convert with .asScala",
    "isEmpty" -> "Dataset.isEmpty is parameterless while RDD.isEmpty() has parens; drop the parens when migrating",
    "++" -> "rename to union (Dataset has no ++ alias)",
    "reduceByKey" -> "key-based aggregation; use a relational groupBy(...).agg(...) or KeyValueGroupedDataset.reduceGroups",
    "groupByKey" -> "RDD.groupByKey differs from Dataset.groupByKey; review the aggregation",
    "aggregateByKey" -> "key-based aggregation; use groupBy(...).agg(...)",
    "combineByKey" -> "key-based aggregation; use groupBy(...).agg(...)",
    "foldByKey" -> "key-based aggregation; use groupBy(...).agg(...)",
    "countByKey" -> "use groupBy(...).count()",
    "countByValue" -> "use groupBy(...).count()",
    "reduceByKeyLocally" -> "key-based aggregation; use groupBy(...).agg(...)",
    "join" -> "extract the join keys and use Dataset.join",
    "leftOuterJoin" -> "extract the join keys and use Dataset.join(..., \"left_outer\")",
    "rightOuterJoin" -> "extract the join keys and use Dataset.join(..., \"right_outer\")",
    "fullOuterJoin" -> "extract the join keys and use Dataset.join(..., \"full_outer\")",
    "cogroup" -> "no direct Dataset equivalent; restructure with joins or groupBy",
    "cartesian" -> "use Dataset.crossJoin",
    "groupBy" -> "RDD.groupBy materializes all values per key; prefer groupBy(...).agg(...)",
    "keyBy" -> "derive the key as a column instead",
    "partitionBy" -> "custom partitioning has no Dataset equivalent",
    "mapValues" -> "pair-RDD function; operate on the value column instead",
    "flatMapValues" -> "pair-RDD function; operate on the value column instead",
    "lookup" -> "use a filter on the key column",
    "sortByKey" -> "use Dataset.orderBy on the key column",
    "zip" -> "RDD.zip has no Dataset equivalent",
    "zipPartitions" -> "no Dataset equivalent",
    "zipWithIndex" -> "use monotonically_increasing_id() or a window function",
    "zipWithUniqueId" -> "use monotonically_increasing_id()",
    "mapPartitionsWithIndex" -> "the partition index is not exposed on Dataset",
    "aggregate" -> "manual aggregation; express it with Dataset.agg",
    "treeAggregate" -> "manual aggregation; express it with Dataset.agg",
    "treeReduce" -> "manual aggregation; express it with Dataset.agg",
    "fold" -> "manual aggregation; express it with Dataset.agg or reduce",
    "glom" -> "no Dataset equivalent",
    "pipe" -> "no Dataset equivalent",
    "saveAsTextFile" -> "use DataFrameWriter, e.g. df.write.text(...)",
    "saveAsObjectFile" -> "use DataFrameWriter, e.g. df.write.parquet(...)",
    "saveAsSequenceFile" -> "use DataFrameWriter",
    "saveAsHadoopFile" -> "use DataFrameWriter",
    "saveAsNewAPIHadoopFile" -> "use DataFrameWriter",
    "saveAsHadoopDataset" -> "use DataFrameWriter",
    "saveAsNewAPIHadoopDataset" -> "use DataFrameWriter"
  )

  /** The operation name if `name` resolves to a member of an RDD class. */
  private def rddOpName(
      name: Term.Name
  )(implicit doc: SemanticDocument): Option[String] = {
    val sym = name.symbol.value
    if (rddOwnerPrefixes.exists(prefix => sym.startsWith(prefix))) Some(name.value)
    else None
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val rddOps: List[(Term.Name, String)] = doc.tree.collect {
      case Term.Select(_, name) => rddOpName(name).map((name, _))
      case Term.ApplyInfix(_, op, _, _) => rddOpName(op).map((op, _))
    }.flatten

    val relevant = rddOps.filterNot { case (_, op) => neutralOps.contains(op) }

    if (relevant.isEmpty) {
      Patch.empty
    } else {
      // Sort by source position so messages / anchors are deterministic.
      val ordered = relevant.sortBy { case (node, _) => node.pos.start }
      val blocking = ordered.filterNot { case (_, op) => simpleOps.contains(op) }
      if (blocking.nonEmpty) {
        blocking.map { case (node, op) =>
          val reason =
            blockingReasons.getOrElse(op, "no known direct Dataset/DataFrame equivalent")
          Patch.lint(RDDMigrationBlocked(node, op, reason))
        }.asPatch
      } else {
        val opNames = ordered.map { case (_, op) => op }.distinct
        Patch.lint(RDDMigrationPossible(ordered.head._1, opNames))
      }
    }
  }
}
