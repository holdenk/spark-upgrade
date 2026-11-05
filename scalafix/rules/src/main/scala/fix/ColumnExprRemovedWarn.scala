package fix

import scalafix.v1._
import scala.meta._
import scala.util.Try

case class ColumnExprRemovedWarning(tn: scala.meta.Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    """Column.expr and new Column(expr: Expression) were removed in Spark 4.0 (SPARK-47933: Column
      |now wraps a ColumnNode). To read the Catalyst expression on Spark 4.x use
      |`import org.apache.spark.sql.classic.ColumnConversions._` (adds `col.expr` / `col.named` via
      |RichColumn and `expression(col)`), or `spark.expression(col)` on an
      |org.apache.spark.sql.classic.SparkSession (cast with ClassicConversions.castToImpl). To build
      |a Column from an Expression, `import org.apache.spark.sql.classic.ClassicConversions._` and
      |write `Column(expr)` instead of `new Column(expr)` -- this only compiles inside an
      |org.apache.spark.* package because the Column companion is private[spark] and ColumnNode is
      |private[sql]; from other packages prefer the public API (functions.expr, Column methods,
      |spark.sql). Note org.apache.spark.sql.classic.ExpressionUtils is private[spark] and NOT
      |callable from user code. If the code must also compile on Spark 3.x, put these calls behind
      |a version shim.""".stripMargin
}

/**
 * Flags `column.expr` (resolved to org.apache.spark.sql.Column#expr) and `new Column(expr)` /
 * `new TypedColumn(expr, enc)` whose constructor takes a Catalyst Expression. `new Column("name")`
 * still exists in 4.0 and is not flagged.
 */
class ColumnExprRemovedWarn extends SemanticRule("ColumnExprRemovedWarn") {
  override val description =
    "Warn on Column.expr and new Column(expr: Expression), removed in Spark 4.0."

  val columnExpr = SymbolMatcher.exact("org/apache/spark/sql/Column#expr.")

  val columnClasses =
    SymbolMatcher.exact("org/apache/spark/sql/Column#", "org/apache/spark/sql/TypedColumn#")

  private val expression = "org/apache/spark/sql/catalyst/expressions/Expression#"
  private val stringTypes = Set("scala/Predef.String#", "java/lang/String#")

  /** Three-valued answer to "is this argument a catalyst Expression?": None means unknown. */
  private type Verdict = Option[Boolean]
  private val isString: Verdict = Some(false)
  private val unknown: Verdict = None

  private def safeSignature(sym: Symbol)(implicit doc: SemanticDocument): Option[Signature] =
    // Classpath lookups throw with an old scalameta on a modern JDK (Scala 2.11 build on JDK 11).
    Try(sym.info.map(_.signature)).toOption.flatten

  /** Walks the parents of a class symbol looking for catalyst's Expression. */
  private def classify(sym: Symbol, seen: Set[String])(implicit doc: SemanticDocument): Verdict = {
    if (sym.value == expression) {
      Some(true)
    } else if (stringTypes.contains(sym.value)) {
      isString
    } else if (seen.contains(sym.value) || seen.size > 64) {
      Some(false)
    } else {
      safeSignature(sym) match {
        case Some(ClassSignature(_, parents, _, _)) =>
          val verdicts = parents.map {
            case TypeRef(_, parent, _) => classify(parent, seen + sym.value)
            case _                     => Some(false)
          }
          if (verdicts.contains(Some(true))) Some(true)
          else if (verdicts.contains(None)) unknown
          else Some(false)
        // A type alias (lower bound == upper bound) stands for its target; a type parameter or
        // an abstract type says nothing about the concrete value, so it is unknown.
        case Some(TypeSignature(_, TypeRef(_, lo, _), TypeRef(_, hi, _))) if lo == hi =>
          classify(lo, seen + sym.value)
        case Some(_: TypeSignature) => unknown
        case _                      => unknown
      }
    }
  }

  private def classifyType(tpe: Type)(implicit doc: SemanticDocument): Verdict =
    classify(tpe.symbol, Set.empty)

  /** Classifies the static type of a term. Shapes that are always Strings need no symbol table. */
  private def classifyTerm(term: Term)(implicit doc: SemanticDocument): Verdict = {
    term match {
      case _: Lit.String                              => isString
      case _: Term.Interpolate                        => isString
      case Term.Ascribe(_, tpe)                       => classifyType(tpe)
      case Term.ApplyInfix(lhs, Term.Name("+"), _, _) =>
        // String concatenation is always a String; anything else with `+` is not an Expression
        // either (Expression has no `+` of its own), but we only trust the String case.
        if (classifyTerm(lhs) == isString) isString else unknown
      case Term.If(_, thenp, elsep) => combine(List(thenp, elsep).map(classifyTerm))
      case Term.Match(_, cases)     => combine(cases.map(c => classifyTerm(c.body)))
      case Term.Block(stats)        =>
        stats.lastOption match {
          case Some(last: Term) => classifyTerm(last)
          case _                => unknown
        }
      case Term.Apply(fun, _) =>
        // Applying a value (Seq, Array, Function, ...) goes through a synthetic `apply` whose
        // result type is an element/type parameter: unknown. Only a method's return type counts.
        safeSignature(fun.symbol) match {
          case Some(MethodSignature(_, _, TypeRef(_, result, _))) => classify(result, Set.empty)
          case Some(_: ClassSignature)                            => companionVerdict(fun.symbol)
          case _                                                  => unknown
        }
      case _ =>
        val sym = term.symbol
        safeSignature(sym) match {
          case Some(ValueSignature(TypeRef(_, tpeSym, _)))        => classify(tpeSym, Set.empty)
          case Some(MethodSignature(_, _, TypeRef(_, tpeSym, _))) => classify(tpeSym, Set.empty)
          case Some(_: ClassSignature)                            => companionVerdict(sym)
          case _                                                  => unknown
        }
    }
  }

  /**
   * `X(...)` on a case class companion resolves to the object (its apply is synthetic), so look at
   * the companion class; `new X(...)` resolves to the class itself.
   */
  private def companionVerdict(sym: Symbol)(implicit doc: SemanticDocument): Verdict = {
    val cls = if (sym.value.endsWith(".")) Symbol(sym.value.dropRight(1) + "#") else sym
    classify(cls, Set.empty)
  }

  /** All branches agree on String -> String; any Expression -> Expression; otherwise unknown. */
  private def combine(verdicts: List[Verdict]): Verdict =
    if (verdicts.contains(Some(true))) Some(true)
    else if (verdicts.nonEmpty && verdicts.forall(_ == isString)) isString
    else unknown

  /**
   * When the argument type cannot be resolved (type parameters, or the Scala 2.11 build on JDK 11
   * where no classpath symbol can be read), only flag code that plausibly deals with catalyst.
   */
  private def plausiblyCatalyst(arg: Term)(implicit doc: SemanticDocument): Boolean =
    doc.input.text.contains("catalyst.expressions") ||
      arg.syntax.contains("Expression") || arg.syntax.contains("catalyst")

  /**
   * `Column(e)` / `TypedColumn(e, enc)` constructor calls (via `new` or an `extends` clause)
   * whose first argument is an Expression. The SemanticDB occurrence on the type name is the
   * class, not the constructor, so the overload is told apart by the static type of the first
   * argument (Column's only other constructor takes a String).
   */
  private def isExpressionConstructor(init: Init)(implicit doc: SemanticDocument): Boolean = {
    init match {
      case Init(tpe, _, (first :: _) :: _) if columnClasses.matches(tpe) =>
        classifyTerm(first) match {
          case Some(verdict) => verdict
          case None          => plausiblyCatalyst(first)
        }
      case _ => false
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    if (doc.input.text.contains("Column") || doc.input.text.contains("expr")) {
      doc.tree.collect {
        case select @ Term.Select(_, name) if columnExpr.matches(name) =>
          Patch.lint(ColumnExprRemovedWarning(select))
        // Covers both `new Column(e)` and `class MyCol(e: Expression) extends Column(e)`.
        case init: Init if isExpressionConstructor(init) =>
          Patch.lint(ColumnExprRemovedWarning(init))
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
