package fix

import scalafix.v1._

import scala.meta._

/**
 * Reported when a file's RDD usage is migratable to the typed Dataset API but
 * the encoders import is missing, so the rewrite can't run yet.
 */
case class RDDMigrationNeedsImplicits(tn: Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    "This RDD pipeline is migratable to the Dataset API, but needs " +
      "`import spark.implicits._` in scope for the encoders. Add it and re-run " +
      "RDDToDatasetMigration to rewrite it automatically."
}

/**
 * Best-effort automatic RDD -> Dataset rewrite (opt-in, `isRewrite`).
 *
 * If a file's RDD usage is entirely composed of operations that map onto the
 * typed `Dataset` API, this rule converts the RDD *origins* to produce a
 * `Dataset` (so the rest of the chain resolves to `Dataset` methods) and renames
 * the few operations whose `Dataset` spelling differs:
 *
 *   - `<sc>.parallelize(seq)` / `makeRDD(seq)` -> `<spark>.createDataset(seq)`
 *     (with `.repartition(n)` appended when a partition count is given)
 *   - `<sc>.textFile(path)`                    -> `<spark>.read.textFile(path)`
 *   - `<dataset>.rdd`                          -> `<dataset>` (drop the `.rdd`)
 *   - `intersection` -> `intersect`, `subtract` -> `except` (renamed in place)
 *
 * Name-identical operations (`map`, `filter`, `flatMap`, `distinct`, `union`,
 * `count`, `collect`, `reduce`, ...) are left untouched.
 *
 * Safety:
 *   - Whole-file gate: if any operation is neither a name-identical swap nor a
 *     known rename (key/pair functions, joins, zips, `sortBy`, RDD-only
 *     accessors, ...), the rule makes NO change and logs each blocker.
 *   - Renames additionally require the file to be "self-contained" (no `RDD[...]`
 *     type and no RDD origin we can't convert, e.g. `objectFile`), so every
 *     renamed operand is guaranteed to become a `Dataset`; otherwise the renamed
 *     ops are logged for manual migration.
 *   - The typed `Dataset` operations need an `Encoder`, i.e. an
 *     `import <session>.implicits._` in scope. The rule does not synthesise it
 *     (a top-level import of a local session wouldn't compile); if it is missing
 *     the file is logged instead of rewritten.
 *
 * The SparkSession is taken from `<x>.sparkContext` when the origin is written
 * that way, otherwise it is assumed to be named `spark` (best effort; the result
 * is meant to be recompiled, which validates the migration).
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
  private val nameIdenticalOps: Set[String] = Set(
    "map", "flatMap", "filter", "mapPartitions",
    "foreach", "foreachPartition",
    "distinct", "union",
    "count", "collect", "toLocalIterator", "take", "first", "reduce", "isEmpty",
    "sample", "cache", "persist", "unpersist", "checkpoint",
    "coalesce", "repartition"
  )

  // RDD operations whose Dataset spelling differs; renamed in place (only when
  // the file is self-contained, see below).
  private val renames: Map[String, String] = Map(
    "intersection" -> "intersect",
    "subtract" -> "except"
  )

  // SparkContext methods that produce an RDD we can't convert to a Dataset, so
  // their presence means the file is not self-contained.
  private val unconvertibleOrigins: Set[String] = Set(
    "objectFile", "sequenceFile", "hadoopFile", "newAPIHadoopFile", "hadoopRDD",
    "newAPIHadoopRDD", "wholeTextFiles", "binaryFiles", "binaryRecords",
    "emptyRDD", "range", "union", "checkpointFile"
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

  private def hasRddType(implicit doc: SemanticDocument): Boolean =
    doc.input.text.contains("org.apache.spark.rdd.RDD") ||
      doc.tree.collect {
        case t @ Type.Name(_) if t.symbol.value.startsWith("org/apache/spark/rdd/RDD#") => t
      }.nonEmpty

  private def hasUnconvertibleOrigin(implicit doc: SemanticDocument): Boolean =
    doc.tree.collect {
      case Term.Select(_, name @ Term.Name(v)) if unconvertibleOrigins(v) && isSparkContextMethod(name) => name
    }.nonEmpty

  // Detects an actual `import <session>.implicits._` (AST, not text, so a comment
  // mentioning implicits doesn't count). Must be robust: a false positive would
  // rewrite without the encoders in scope and not compile.
  private def hasImplicitsImport(implicit doc: SemanticDocument): Boolean =
    doc.tree.collect {
      case Importer(Term.Select(_, Term.Name("implicits")), importees)
          if importees.exists {
            case Importee.Wildcard() => true
            case _ => false
          } =>
        true
    }.nonEmpty

  private def originPatches(implicit doc: SemanticDocument): List[Patch] =
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
    }

  private def renamePatches(implicit doc: SemanticDocument): List[Patch] =
    doc.tree.collect {
      case Term.Select(_, name @ Term.Name(v)) if renames.contains(v) && methodOwnedBy(name, rddOwnerPrefixes) =>
        Patch.replaceTree(name, renames(v))
    }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val rddOps: List[(Term.Name, String)] = doc.tree.collect {
      case Term.Select(_, name) => rddOpName(name).map((name, _))
      case Term.ApplyInfix(_, op, _, _) => rddOpName(op).map((op, _))
    }.flatten

    if (rddOps.isEmpty) {
      Patch.empty
    } else {
      val blockers = rddOps.filterNot { case (_, op) =>
        nameIdenticalOps.contains(op) || renames.contains(op)
      }
      val renameOps = rddOps.filter { case (_, op) => renames.contains(op) }
      if (blockers.nonEmpty) {
        // Has a genuinely non-migratable op: log every blocker, change nothing.
        blockers.map { case (node, op) =>
          Patch.lint(
            RDDMigrationBlocked(
              node,
              op,
              "operation has no automatic Dataset rewrite; migrate it manually or leave it as an RDD"
            )
          )
        }.asPatch
      } else if (renameOps.nonEmpty && (hasRddType || hasUnconvertibleOrigin)) {
        // Renames need every operand to become a Dataset, which we can't
        // guarantee when an RDD comes from a non-convertible origin.
        renameOps.map { case (node, op) =>
          Patch.lint(
            RDDMigrationBlocked(
              node,
              op,
              s"rewrite to Dataset.${renames(op)} manually -- an RDD here has a non-convertible origin"
            )
          )
        }.asPatch
      } else if (!hasImplicitsImport) {
        // Migratable, but the Dataset encoders import is missing.
        Patch.lint(RDDMigrationNeedsImplicits(rddOps.head._1))
      } else {
        (originPatches ++ renamePatches).asPatch
      }
    }
  }
}
