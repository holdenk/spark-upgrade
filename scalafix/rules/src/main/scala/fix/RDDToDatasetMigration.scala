package fix

import scalafix.v1._

import scala.meta._

/**
 * Best-effort automatic RDD -> Dataset rewrite (opt-in, `isRewrite`).
 *
 * If a file's RDD usage is entirely composed of operations that exist on the
 * typed `Dataset` API with the same name and a compatible signature, this rule
 * rewrites the RDD *origins* to produce a `Dataset` (so the rest of the chain
 * resolves to `Dataset` methods) and leaves the operations untouched:
 *
 *   - `<sc>.parallelize(seq)` / `makeRDD(seq)` -> `<spark>.createDataset(seq)`
 *     (with `.repartition(n)` appended when a partition count is given)
 *   - `<sc>.textFile(path)`                    -> `<spark>.read.textFile(path)`
 *   - `<dataset>.rdd`                          -> `<dataset>` (drop the `.rdd`)
 *
 * The SparkSession is taken from `<x>.sparkContext` when the origin is written
 * that way, otherwise it is assumed to be named `spark` (best effort -- the
 * result is meant to be recompiled, which validates the migration). The typed
 * `Dataset` operations and `createDataset` require an `Encoder`, i.e. an
 * `import spark.implicits._` in scope; the rule does not synthesise that import
 * (it can't place a local import reliably) so it must already be present.
 *
 * If the file uses any operation that is NOT a direct name-for-name swap
 * (key/pair functions, joins, zips, manual aggregations, `sortBy`,
 * `intersection`/`subtract`/`++`, RDD-only accessors, ...) the rule makes NO
 * change and instead emits a [[RDDMigrationBlocked]] lint at each such
 * operation -- i.e. it only swaps when the whole pipeline is migratable.
 */
class RDDToDatasetMigration extends SemanticRule("RDDToDatasetMigration") {
  override val isRewrite: Boolean = true

  override val description: String =
    "Rewrites a fully-migratable RDD pipeline to the typed Dataset API; logs the blockers otherwise."

  private val rddOwnerPrefixes: List[String] = List(
    "org/apache/spark/rdd/RDD#",
    "org/apache/spark/rdd/PairRDDFunctions#",
    "org/apache/spark/rdd/OrderedRDDFunctions#",
    "org/apache/spark/rdd/DoubleRDDFunctions#",
    "org/apache/spark/rdd/SequenceFileRDDFunctions#",
    "org/apache/spark/rdd/AsyncRDDActions#"
  )

  // RDD operations that exist on Dataset with the same name and a compatible
  // signature, so the call text can be left as-is once the receiver is a Dataset.
  private val swappableOps: Set[String] = Set(
    "map", "flatMap", "filter", "mapPartitions",
    "foreach", "foreachPartition",
    "distinct", "union",
    "count", "collect", "toLocalIterator", "take", "first", "reduce", "isEmpty",
    "sample", "cache", "persist", "unpersist", "checkpoint",
    "coalesce", "repartition"
  )

  // Operations that are migratable in principle but are not a name-for-name swap,
  // so they block the automatic rewrite (the file is logged, not rewritten).
  private val manualReasons: Map[String, String] = Map(
    "intersection" -> "rewrite to Dataset.intersect",
    "subtract" -> "rewrite to Dataset.except",
    "++" -> "rewrite to Dataset.union",
    "sortBy" -> "rewrite to Dataset.orderBy / sort with a column expression"
  )

  private def methodOwnedBy(name: Term.Name, prefixes: List[String])(implicit
      doc: SemanticDocument
  ): Boolean = {
    val sym = name.symbol.value
    prefixes.exists(p => sym.startsWith(p))
  }

  private def rddOpName(name: Term.Name)(implicit doc: SemanticDocument): Option[String] =
    if (methodOwnedBy(name, rddOwnerPrefixes)) Some(name.value) else None

  private def isSparkContextMethod(name: Term.Name)(implicit doc: SemanticDocument): Boolean =
    methodOwnedBy(name, List("org/apache/spark/SparkContext#"))

  private def isDatasetRdd(name: Term.Name)(implicit doc: SemanticDocument): Boolean =
    methodOwnedBy(name, List("org/apache/spark/sql/Dataset#rdd"))

  /** The SparkSession to drive createDataset/read from; best-effort. */
  private def sessionName(scExpr: Term): String =
    scExpr match {
      case Term.Select(session, Term.Name("sparkContext")) => session.syntax
      case _ => "spark"
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val rddOps: List[(Term.Name, String)] = doc.tree.collect {
      case Term.Select(_, name) => rddOpName(name).map((name, _))
      case Term.ApplyInfix(_, op, _, _) => rddOpName(op).map((op, _))
    }.flatten

    if (rddOps.isEmpty) {
      Patch.empty
    } else {
      val nonSwappable = rddOps.filterNot { case (_, op) => swappableOps.contains(op) }
      if (nonSwappable.nonEmpty) {
        // Not fully migratable: log the blockers, change nothing.
        nonSwappable.map { case (node, op) =>
          val reason = manualReasons.getOrElse(
            op,
            "operation has no automatic Dataset rewrite; migrate it manually or leave it as an RDD"
          )
          Patch.lint(RDDMigrationBlocked(node, op, reason))
        }.asPatch
      } else {
        // Fully migratable: convert the RDD origins to Datasets.
        doc.tree.collect {
          case t @ Term.Apply(Term.Select(scExpr, name @ Term.Name(m)), args)
              if (m == "parallelize" || m == "makeRDD") && isSparkContextMethod(name) =>
            val base = s"${sessionName(scExpr)}.createDataset(${args.head.syntax})"
            val repl = if (args.lengthCompare(2) >= 0) s"$base.repartition(${args(1).syntax})" else base
            Patch.replaceTree(t, repl)
          case t @ Term.Apply(Term.Select(scExpr, name @ Term.Name("textFile")), args)
              if isSparkContextMethod(name) =>
            Patch.replaceTree(t, s"${sessionName(scExpr)}.read.textFile(${args.head.syntax})")
          case sel @ Term.Select(qual, name @ Term.Name("rdd")) if isDatasetRdd(name) =>
            Patch.replaceTree(sel, qual.syntax)
        }.asPatch
      }
    }
  }
}
