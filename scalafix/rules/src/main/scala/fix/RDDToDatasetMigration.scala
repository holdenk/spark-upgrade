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
 *   - `intersection` -> `intersect`, `subtract` -> `exceptAll`
 *
 * Name-identical operations (`map`, `filter`, `flatMap`, `distinct`, `union`,
 * `count`, `collect`, `reduce`, ...) are left untouched.
 *
 * Safety:
 *   - Whole-file gate: if any operation is neither a name-identical swap nor a
 *     known rename (key/pair functions, joins, zips, `sortBy`, RDD-only
 *     accessors, ...), the rule makes NO change and logs each blocker. Ops whose
 *     `Dataset` namesake has a different signature only at a higher arity
 *     (`coalesce(n, shuffle)`, `distinct(n)`, `mapPartitions(f, preserves)`) are
 *     also blocked at that arity (see `maxSafeArgs`); the safe arity is rewritten.
 *   - Renames additionally require both operands of each renamed op to *trace*
 *     to a convertible origin (so they are guaranteed to become `Dataset`s);
 *     otherwise the rename is logged for manual migration. This is checked per
 *     operand (`tracesToDataset`), not by a coarse whole-file heuristic, so an
 *     RDD that arrives from an unknown source (a parameter, a non-Spark helper)
 *     correctly blocks the rename.
 *   - The typed `Dataset` operations need an `Encoder`, i.e. an
 *     `import <session>.implicits._` in scope. The rule does not synthesise it
 *     (a top-level import of a local session wouldn't compile); if it is missing
 *     the file is logged instead of rewritten.
 *
 * `createDataset`/`read` are driven by the session named in `<x>.sparkContext`
 * when the origin is written that way, otherwise by the session whose
 * `implicits._` are imported (the encoders and `createDataset` must come from
 * the same session); the result is meant to be recompiled, which validates it.
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
  // signature/return type, so the call text can be left as-is once the receiver
  // is a Dataset. NOTE: `toLocalIterator` (Dataset returns a java.util.Iterator,
  // not a scala Iterator) and `checkpoint` (Dataset returns a *new* checkpointed
  // Dataset instead of mutating in place) are deliberately NOT here -- their
  // Dataset namesakes differ even at the base arity.
  private val nameIdenticalOps: Set[String] = Set(
    "map", "flatMap", "filter", "mapPartitions",
    "foreach", "foreachPartition",
    "distinct", "union",
    "count", "collect", "take", "first", "reduce", "isEmpty",
    "sample", "cache", "persist", "unpersist",
    "coalesce", "repartition"
  )

  // Ops that match Dataset only up to a maximum argument count; called with more
  // args the Dataset signature differs (won't compile), so block at that arity.
  private val maxSafeArgs: Map[String, Int] = Map(
    "coalesce" -> 1,      // Dataset.coalesce(n); RDD.coalesce(n, shuffle)
    "distinct" -> 0,      // Dataset.distinct();  RDD.distinct(n)
    "mapPartitions" -> 1  // Dataset.mapPartitions(f); RDD.mapPartitions(f, preserves)
  )

  // RDD operations whose Dataset spelling differs; renamed in place (only when
  // both operands trace to a convertible origin, see `tracesToDataset`).
  private val renames: Map[String, String] = Map(
    "intersection" -> "intersect", // both dedup (INTERSECT DISTINCT) -- semantics match
    "subtract" -> "exceptAll"      // RDD.subtract keeps dups; EXCEPT ALL keeps dups (NOT `except`)
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

  /** One operation call on an RDD: the op name node, its name, receiver, and args. */
  private case class OpSite(node: Term.Name, op: String, receiver: Term, args: List[Term])

  /** The session whose `implicits._` are imported, if any (e.g. `import ss.implicits._` -> "ss"). */
  private def implicitsSessionName(implicit doc: SemanticDocument): Option[String] =
    doc.tree.collect {
      case Importer(Term.Select(prefix, Term.Name("implicits")), importees)
          if importees.exists(_.is[Importee.Wildcard]) =>
        prefix.syntax
    }.headOption

  /** The session to drive createDataset/read from for a given origin receiver. */
  private def sessionName(scExpr: Term)(implicit doc: SemanticDocument): String =
    scExpr match {
      case Term.Select(session, Term.Name("sparkContext")) => session.syntax
      case _ => implicitsSessionName.getOrElse("spark")
    }

  /** True if `t` is an origin call this rule converts to a Dataset. */
  private def isConvertibleOriginCall(t: Term)(implicit doc: SemanticDocument): Boolean =
    t match {
      case Term.Apply(Term.Select(_, name @ Term.Name(m)), _)
          if (m == "parallelize" || m == "makeRDD" || m == "textFile") && isSparkContextMethod(name) =>
        true
      case Term.Select(_, name @ Term.Name("rdd")) if isDatasetRdd(name) => true
      case _ => false
    }

  /** The right-hand side of a local `val`/`var` binding the given name, if present. */
  private def valDefRhs(name: String)(implicit doc: SemanticDocument): Option[Term] =
    doc.tree.collect {
      case Defn.Val(_, List(Pat.Var(Term.Name(n))), _, rhs) if n == name        => rhs
      case Defn.Var(_, List(Pat.Var(Term.Name(n))), _, Some(rhs)) if n == name   => rhs
    }.headOption

  /**
   * True if `t` is (or resolves to) a value the rewrite turns into a Dataset:
   * a convertible origin, a chain of RDD ops on such a value, or a local val
   * bound to one. Conservatively false for parameters / unknown sources.
   */
  private def tracesToDataset(t: Term, seen: Set[String])(implicit doc: SemanticDocument): Boolean =
    if (isConvertibleOriginCall(t)) true
    else
      t match {
        case Term.Name(v) if !seen(v) =>
          valDefRhs(v).exists(rhs => tracesToDataset(rhs, seen + v))
        case Term.Apply(Term.Select(recv, name), _) if rddOpName(name).isDefined =>
          tracesToDataset(recv, seen)
        case Term.ApplyInfix(lhs, op, _, _) if rddOpName(op).isDefined =>
          tracesToDataset(lhs, seen)
        case Term.Select(recv, name) if rddOpName(name).isDefined =>
          tracesToDataset(recv, seen)
        case _ => false
      }

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
      case Term.ApplyInfix(_, op @ Term.Name(v), _, _) if renames.contains(v) && methodOwnedBy(op, rddOwnerPrefixes) =>
        Patch.replaceTree(op, renames(v))
    }

  // Detects an actual `import <session>.implicits._` (AST, not text, so a comment
  // mentioning implicits doesn't count). Must be robust: a false positive would
  // rewrite without the encoders in scope and not compile.
  private def hasImplicitsImport(implicit doc: SemanticDocument): Boolean =
    implicitsSessionName.isDefined

  override def fix(implicit doc: SemanticDocument): Patch = {
    val rddOps: List[OpSite] = doc.tree.collect {
      case Term.Apply(Term.Select(recv, name), args) if rddOpName(name).isDefined =>
        OpSite(name, name.value, recv, args)
      case Term.ApplyInfix(lhs, op, _, args) if rddOpName(op).isDefined =>
        OpSite(op, op.value, lhs, args)
      case sel @ Term.Select(recv, name)
          if rddOpName(name).isDefined && !sel.parent.exists(_.is[Term.Apply]) =>
        OpSite(name, name.value, recv, Nil)
    }

    if (rddOps.isEmpty) {
      Patch.empty
    } else {
      def arityBad(o: OpSite): Boolean = maxSafeArgs.get(o.op).exists(max => o.args.lengthCompare(max) > 0)
      val blockers = rddOps.filter { o =>
        (!nameIdenticalOps.contains(o.op) && !renames.contains(o.op)) || arityBad(o)
      }
      val renameOps = rddOps.filter(o => renames.contains(o.op))
      if (blockers.nonEmpty) {
        // Has a genuinely non-migratable op (or unsupported arity): log, change nothing.
        blockers.map { o =>
          val reason =
            if (arityBad(o)) s"Dataset.${o.op} does not accept this many arguments; migrate manually"
            else "operation has no automatic Dataset rewrite; migrate it manually or leave it as an RDD"
          Patch.lint(RDDMigrationBlocked(o.node, o.op, reason))
        }.asPatch
      } else {
        val unsafeRenames = renameOps.filterNot { o =>
          tracesToDataset(o.receiver, Set.empty) && o.args.forall(a => tracesToDataset(a, Set.empty))
        }
        if (unsafeRenames.nonEmpty) {
          // A renamed op has an operand we can't guarantee becomes a Dataset.
          unsafeRenames.map { o =>
            Patch.lint(
              RDDMigrationBlocked(
                o.node,
                o.op,
                s"rewrite to Dataset.${renames(o.op)} manually -- an operand does not trace to a convertible origin"
              )
            )
          }.asPatch
        } else if (!hasImplicitsImport) {
          // Migratable, but the Dataset encoders import is missing.
          Patch.lint(RDDMigrationNeedsImplicits(rddOps.head.node))
        } else {
          (originPatches ++ renamePatches).asPatch
        }
      }
    }
  }
}
