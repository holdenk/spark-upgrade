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
      "`import <session>.implicits._` in scope at this call site for the encoders. " +
      "Add it and re-run RDDToDatasetMigration to rewrite it automatically."
}

/**
 * Best-effort automatic RDD -> Dataset rewrite (opt-in, `isRewrite`).
 *
 * The rule is deliberately CONSERVATIVE: it rewrites a file only when it can do
 * so without changing what the code compiles to OR what it computes. The safe
 * surface is small, and anything outside it is logged for manual migration
 * rather than rewritten into code that might not compile or might silently
 * change results.
 *
 * When safe, it converts the RDD *origins* so the chain resolves to `Dataset`
 * methods, and renames the one operation whose `Dataset` spelling differs:
 *
 *   - `<sess>.sparkContext.parallelize(seq)` / `makeRDD(seq)` -> `<sess>.createDataset(seq)`
 *   - `<sess>.sparkContext.textFile(path)`                    -> `<sess>.read.textFile(path)`
 *   - `<dataset>.rdd`                                         -> `<dataset>` (drop the `.rdd`)
 *   - `intersection` -> `intersect` (both dedup; semantics match)
 *
 * Only the SINGLE-argument origin forms are converted: `parallelize(seq, n)` and
 * `textFile(path, minPartitions)` are blocked, because `Dataset` cannot reproduce
 * RDD partition slicing without a shuffle that reorders rows / drops the hint.
 *
 * Name-identical operations (`map`, `filter`, `flatMap`, `count`, `collect`,
 * `reduce`, `union`, ...) keep their call text. `union` and `intersection` are
 * binary, so their argument must also trace to a convertible origin (else the
 * argument would still be an RDD and the call would not compile).
 *
 * Blocked (logged, never rewritten) -- the `Dataset` namesake differs in a way
 * that would break compilation or change results:
 *   - `subtract` (RDD removes all rows whose value is in the other RDD; no single
 *     Dataset op matches -- needs a left-anti join), `sample` (different sampler/
 *     seed), `isEmpty` (Dataset.isEmpty is parameterless), `toLocalIterator`
 *     (returns a java.util.Iterator), `checkpoint` (returns a new Dataset).
 *   - higher arities of `coalesce`/`distinct`/`mapPartitions` (no Dataset overload).
 *   - any op used as an eta-expanded method value (`rdd.map _`: Dataset.map is
 *     overloaded, so the eta form is ambiguous).
 *   - any file that declares an `RDD[...]` type (a return type / ascription would
 *     be left dangling once the value becomes a Dataset).
 *
 * The session that drives `createDataset`/`read` is `<x>.sparkContext`'s receiver
 * when the origin is written that way, else the session whose `implicits._` are
 * imported; a file with more than one `implicits._` import is blocked as
 * ambiguous. The `implicits._` import must already be LEXICALLY IN SCOPE at each
 * rewritten site that needs an `Encoder` (`createDataset` origins and
 * `map`/`flatMap`/`mapPartitions`) -- a file-wide import check is not enough, and
 * the import is not synthesised (a top-level import of a local session wouldn't
 * compile). An explicit origin type argument is preserved
 * (`parallelize[T](seq)` -> `createDataset[T](seq)`), keeping the empty-seq idiom
 * compiling. Known limitation: a type alias of `RDD` (`type MyRDD = RDD[Int]`)
 * used as an annotation is not detected by the dangling-type guard.
 */
class RDDToDatasetMigration extends SemanticRule("RDDToDatasetMigration") {
  override val isRewrite: Boolean = true

  override val description: String =
    "Conservatively rewrites a fully-migratable RDD pipeline to the typed Dataset API; logs the blockers otherwise."

  private val rddOwnerPrefixes: List[String] = List(
    "org/apache/spark/rdd/RDD#",
    "org/apache/spark/rdd/PairRDDFunctions#",
    "org/apache/spark/rdd/OrderedRDDFunctions#",
    "org/apache/spark/rdd/DoubleRDDFunctions#",
    "org/apache/spark/rdd/SequenceFileRDDFunctions#",
    "org/apache/spark/rdd/AsyncRDDActions#"
  )

  // RDD operations that exist on Dataset with the same name, arity, return type
  // AND runtime semantics, so the call text can be left as-is once the receiver
  // is a Dataset. Audited against the Spark 3.x Dataset/RDD APIs.
  private val nameIdenticalOps: Set[String] = Set(
    "map", "flatMap", "filter", "mapPartitions",
    "foreach", "foreachPartition",
    "distinct", "union",
    "count", "collect", "take", "first", "reduce",
    "cache", "persist", "unpersist",
    "coalesce", "repartition"
  )

  // Binary ops: the argument is another RDD, which must also become a Dataset,
  // so it must trace to a convertible origin (`union` is name-identical,
  // `intersection` is a rename; both need the operand check).
  private val binaryDatasetArgOps: Set[String] = Set("union", "intersection")

  // Ops that match Dataset only up to a maximum argument count; beyond it the
  // Dataset signature differs (won't compile), so block at that arity.
  private val maxSafeArgs: Map[String, Int] = Map(
    "coalesce" -> 1,      // Dataset.coalesce(n); RDD.coalesce(n, shuffle)
    "distinct" -> 0,      // Dataset.distinct();  RDD.distinct(n)
    "mapPartitions" -> 1  // Dataset.mapPartitions(f); RDD.mapPartitions(f, preserves)
  )

  // RDD operations whose Dataset spelling differs but is faithful; renamed in place.
  private val renames: Map[String, String] = Map(
    "intersection" -> "intersect" // both INTERSECT DISTINCT -- semantics match
  )

  // Ops with a same-name Dataset method that is NOT a safe swap; logged with
  // specific guidance instead of being rewritten.
  private val manualReasons: Map[String, String] = Map(
    "subtract" -> "RDD.subtract removes every row whose value appears in the other RDD (key removal); no single Dataset op matches -- use a left-anti join",
    "sample" -> "Dataset.sample uses a different sampler and seed derivation, so the same seed yields different rows -- migrate by hand",
    "isEmpty" -> "Dataset.isEmpty is parameterless while RDD.isEmpty() has parens -- migrate by hand",
    "toLocalIterator" -> "Dataset.toLocalIterator returns a java.util.Iterator, not a scala Iterator -- migrate by hand",
    "checkpoint" -> "Dataset.checkpoint returns a new checkpointed Dataset instead of mutating in place -- migrate by hand"
  )

  private val genericReason =
    "operation has no automatic Dataset rewrite; migrate it manually or leave it as an RDD"

  /** One operation call on an RDD: the op name node, its name, receiver, and args. */
  private case class OpSite(node: Term.Name, op: String, receiver: Term, args: List[Term])

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

  /** Prefixes of every `import <x>.implicits._` in the file (`<x>` is the session). */
  private def implicitsSessions(implicit doc: SemanticDocument): List[String] =
    doc.tree.collect {
      case Importer(Term.Select(prefix, Term.Name("implicits")), importees)
          if importees.exists(_.is[Importee.Wildcard]) =>
        prefix.syntax
    }

  private def sessionName(scExpr: Term)(implicit doc: SemanticDocument): String =
    scExpr match {
      case Term.Select(session, Term.Name("sparkContext")) => session.syntax
      case _ => implicitsSessions.headOption.getOrElse("spark")
    }

  /**
   * True if the file uses an `RDD[...]` *type* (a return type / ascription / param).
   * Matched on the resolved `Type.Name` symbol, NOT on source text: an unused
   * `import org.apache.spark.rdd.RDD`, a comment, or a string literal mentioning the
   * FQCN is not a dangling type and must not block a safe rewrite.
   */
  private def hasRddType(implicit doc: SemanticDocument): Boolean =
    doc.tree.collect {
      case t @ Type.Name(_) if t.symbol.value.startsWith("org/apache/spark/rdd/RDD#") => t
    }.nonEmpty

  /** RDD ops used as an eta-expanded method value (`rdd.map _`), which can't be swapped safely. */
  private def etaOps(implicit doc: SemanticDocument): List[Term.Name] =
    doc.tree.collect {
      case Term.Eta(Term.Select(_, name)) if rddOpName(name).isDefined => name
      case Term.Eta(Term.ApplyType(Term.Select(_, name), _)) if rddOpName(name).isDefined => name
    }

  private def hasNamedArg(args: List[Term]): Boolean = args.exists(_.is[Term.Assign])

  /** Origin calls whose argument form we can't safely convert (only single positional arg is supported). */
  private def badShapeOrigins(implicit doc: SemanticDocument): List[(Term.Name, String)] =
    doc.tree.collect {
      case Term.Apply(Term.Select(_, name @ Term.Name(m)), args)
          if (m == "parallelize" || m == "makeRDD" || m == "textFile") &&
            isSparkContextMethod(name) && (args.lengthCompare(1) != 0 || hasNamedArg(args)) =>
        (name, m)
      case Term.Apply(Term.ApplyType(Term.Select(_, name @ Term.Name(m)), _), args)
          if (m == "parallelize" || m == "makeRDD" || m == "textFile") &&
            isSparkContextMethod(name) && (args.lengthCompare(1) != 0 || hasNamedArg(args)) =>
        (name, m)
    }

  /** True if `t` is an origin call this rule converts to a Dataset. */
  private def isConvertibleOriginCall(t: Term)(implicit doc: SemanticDocument): Boolean =
    t match {
      case Term.Apply(Term.Select(_, name @ Term.Name(m)), _)
          if (m == "parallelize" || m == "makeRDD" || m == "textFile") && isSparkContextMethod(name) =>
        true
      case Term.Apply(Term.ApplyType(Term.Select(_, name @ Term.Name(m)), _), _)
          if (m == "parallelize" || m == "makeRDD" || m == "textFile") && isSparkContextMethod(name) =>
        true
      case Term.Select(_, name @ Term.Name("rdd")) if isDatasetRdd(name) => true
      case _ => false
    }

  /** The right-hand side of a local `val`/`var` binding the given name, if present. */
  private def valDefRhs(name: String)(implicit doc: SemanticDocument): Option[Term] =
    doc.tree.collect {
      case Defn.Val(_, List(Pat.Var(Term.Name(n))), _, rhs) if n == name      => rhs
      case Defn.Var(_, List(Pat.Var(Term.Name(n))), _, Some(rhs)) if n == name => rhs
    }.headOption

  /**
   * True if `t` is (or resolves to) a value the rewrite turns into a Dataset:
   * a convertible origin, a chain of RDD ops on one, or a local val bound to one.
   * Conservatively false for parameters / unknown sources.
   */
  private def tracesToDataset(t: Term, seen: Set[String])(implicit doc: SemanticDocument): Boolean =
    if (isConvertibleOriginCall(t)) true
    else
      t match {
        case Term.Name(v) if !seen(v) =>
          valDefRhs(v).exists(rhs => tracesToDataset(rhs, seen + v))
        case Term.Apply(Term.Select(recv, name), _) if rddOpName(name).isDefined =>
          tracesToDataset(recv, seen)
        case Term.Apply(Term.ApplyType(Term.Select(recv, name), _), _) if rddOpName(name).isDefined =>
          tracesToDataset(recv, seen)
        case Term.ApplyInfix(lhs, op, _, _) if rddOpName(op).isDefined =>
          tracesToDataset(lhs, seen)
        case Term.Select(recv, name) if rddOpName(name).isDefined =>
          tracesToDataset(recv, seen)
        case _ => false
      }

  // Both the plain `parallelize(seq)` and the explicit-type-argument `parallelize[T](seq)`
  // forms are converted; the latter parses as Term.Apply(Term.ApplyType(Term.Select(...))).
  // A written type argument is PRESERVED (`createDataset[T](...)`): dropping it would
  // re-infer T from the argument alone, which breaks the empty-seq idiom
  // (`parallelize[Int](Seq())` must not become `createDataset(Seq())`, whose T is
  // Nothing). Keep this in lockstep with isConvertibleOriginCall / badShapeOrigins.
  private def originReplacement(scExpr: Term, m: String, targs: List[Type], args: List[Term])(implicit
      doc: SemanticDocument
  ): String = {
    val targsStr = if (targs.isEmpty) "" else targs.map(_.syntax).mkString("[", ", ", "]")
    if (m == "textFile") s"${sessionName(scExpr)}.read.textFile(${args.head.syntax})"
    else s"${sessionName(scExpr)}.createDataset$targsStr(${args.head.syntax})"
  }

  private def originPatches(implicit doc: SemanticDocument): List[Patch] =
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(scExpr, name @ Term.Name(m)), args)
          if (m == "parallelize" || m == "makeRDD" || m == "textFile") && isSparkContextMethod(name) &&
            args.lengthCompare(1) == 0 && !hasNamedArg(args) =>
        Patch.replaceTree(t, originReplacement(scExpr, m, Nil, args))
      case t @ Term.Apply(Term.ApplyType(Term.Select(scExpr, name @ Term.Name(m)), targs), args)
          if (m == "parallelize" || m == "makeRDD" || m == "textFile") && isSparkContextMethod(name) &&
            args.lengthCompare(1) == 0 && !hasNamedArg(args) =>
        Patch.replaceTree(t, originReplacement(scExpr, m, targs, args))
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

  // Dataset ops whose rewritten form needs an implicit Encoder at the call site.
  private val encoderNeedingOps: Set[String] = Set("map", "flatMap", "mapPartitions")

  private def isSparkContextReceiver(scExpr: Term): Boolean =
    scExpr match {
      case Term.Select(_, Term.Name("sparkContext")) => true
      case _ => false
    }

  /**
   * Sites whose rewritten form needs the session's `implicits._` lexically in
   * scope right there: `createDataset` origins (`Encoder[T]`), the
   * `map`/`flatMap`/`mapPartitions` calls (`Encoder[U]`), and bare-receiver
   * `textFile` origins (the emitted session name is the implicits import's
   * prefix, so that import must be visible at the site).
   */
  private def encoderSites(rddOps: List[OpSite])(implicit doc: SemanticDocument): List[Tree] = {
    def needsImplicits(m: String, scExpr: Term): Boolean =
      m == "parallelize" || m == "makeRDD" || (m == "textFile" && !isSparkContextReceiver(scExpr))
    val originSites = doc.tree.collect {
      case Term.Apply(Term.Select(scExpr, name @ Term.Name(m)), _)
          if needsImplicits(m, scExpr) && isSparkContextMethod(name) =>
        name: Tree
      case Term.Apply(Term.ApplyType(Term.Select(scExpr, name @ Term.Name(m)), _), _)
          if needsImplicits(m, scExpr) && isSparkContextMethod(name) =>
        name: Tree
    }
    originSites ++ rddOps.filter(o => encoderNeedingOps(o.op)).map(o => o.node: Tree)
  }

  private def isImplicitsImportStat(s: Stat): Boolean =
    s match {
      case Import(importers) =>
        importers.exists {
          case Importer(Term.Select(_, Term.Name("implicits")), importees) =>
            importees.exists(_.is[Importee.Wildcard])
          case _ => false
        }
      case _ => false
    }

  /**
   * True if an `import <session>.implicits._` is lexically in scope at `t`: some
   * enclosing block/template/source contains that import before `t`. A file-wide
   * check is not enough -- an import inside one method puts neither the encoders
   * nor the session name in scope in another method.
   */
  private def implicitsInScopeAt(t: Tree): Boolean = {
    def enclosingStats(anc: Tree): List[Stat] = anc match {
      case b: Term.Block => b.stats
      case tpl: Template => tpl.stats
      case s: Source => s.stats
      case p: Pkg => p.stats
      case _ => Nil
    }
    var cur = t.parent
    var found = false
    while (!found && cur.isDefined) {
      val anc = cur.get
      found = enclosingStats(anc).exists(s => isImplicitsImportStat(s) && s.pos.end <= t.pos.start)
      cur = anc.parent
    }
    found
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val rddOps: List[OpSite] = doc.tree.collect {
      case Term.Apply(Term.Select(recv, name), args) if rddOpName(name).isDefined =>
        OpSite(name, name.value, recv, args)
      case Term.Apply(Term.ApplyType(Term.Select(recv, name), _), args) if rddOpName(name).isDefined =>
        OpSite(name, name.value, recv, args)
      case Term.ApplyInfix(lhs, op, _, args) if rddOpName(op).isDefined =>
        OpSite(op, op.value, lhs, args)
      case sel @ Term.Select(recv, name)
          if rddOpName(name).isDefined &&
            !sel.parent.exists(p => p.is[Term.Apply] || p.is[Term.ApplyType] || p.is[Term.Eta]) =>
        OpSite(name, name.value, recv, Nil)
    }
    val etas = etaOps

    if (rddOps.isEmpty && etas.isEmpty) {
      Patch.empty
    } else {
      def arityBad(o: OpSite): Boolean = maxSafeArgs.get(o.op).exists(max => o.args.lengthCompare(max) > 0)

      val anchor: Tree = (rddOps.map(_.node) ++ etas).head

      // Blocker categories, in priority order; the first non-empty one is reported.
      val unsupported: List[(Tree, String, String)] =
        rddOps.filter(o => !nameIdenticalOps.contains(o.op) && !renames.contains(o.op))
          .map(o => (o.node: Tree, o.op, manualReasons.getOrElse(o.op, genericReason)))
      val arity: List[(Tree, String, String)] =
        rddOps.filter(arityBad)
          .map(o => (o.node: Tree, o.op, s"Dataset.${o.op} does not accept this many arguments; migrate manually"))
      val badOrigins: List[(Tree, String, String)] =
        badShapeOrigins.map { case (n, m) =>
          (n: Tree, m, s"only the single-argument $m form is converted (Dataset can't reproduce RDD partition slicing); migrate manually")
        }
      val unsafeBinary: List[(Tree, String, String)] =
        rddOps.filter(o => binaryDatasetArgOps.contains(o.op))
          .filterNot(o => tracesToDataset(o.receiver, Set.empty) && o.args.forall(a => tracesToDataset(a, Set.empty)))
          .map(o => (o.node: Tree, o.op, s"an operand of ${o.op} does not trace to a convertible origin, so it would remain an RDD; migrate manually"))
      val etaBlocks: List[(Tree, String, String)] =
        etas.map(n => (n: Tree, n.value, s"${n.value} is used as a method value (eta-expansion); the Dataset method is overloaded, so this is ambiguous -- migrate manually"))
      val rddTypeBlock: List[(Tree, String, String)] =
        if (hasRddType) List((anchor, "RDD", "this file declares an RDD[...] type; rewriting the chain to a Dataset would leave that annotation dangling -- migrate manually"))
        else Nil
      val ambiguousSession: List[(Tree, String, String)] =
        // Count DISTINCT sessions, not total imports: the same `import s.implicits._`
        // repeated across methods (the norm once the import must be in scope per site)
        // is one unambiguous session, not two.
        if (implicitsSessions.distinct.lengthCompare(1) > 0) List((anchor, "implicits", "more than one distinct `implicits._` session makes the target SparkSession ambiguous; migrate manually"))
        else Nil

      val blockers = List(unsupported, arity, badOrigins, unsafeBinary, etaBlocks, rddTypeBlock, ambiguousSession)
        .find(_.nonEmpty)
        .getOrElse(Nil)

      if (blockers.nonEmpty) {
        blockers.map { case (n, op, reason) => Patch.lint(RDDMigrationBlocked(n, op, reason)) }.asPatch
      } else {
        // Per-site, not file-wide: an import inside another method wouldn't put
        // the encoders (or the session name) in scope at this call.
        val missingEncoders = encoderSites(rddOps).filterNot(implicitsInScopeAt)
        if (missingEncoders.nonEmpty) Patch.lint(RDDMigrationNeedsImplicits(missingEncoders.head))
        else (originPatches ++ renamePatches).asPatch
      }
    }
  }
}
