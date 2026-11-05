package fix

import scalafix.v1._
import scala.meta._
import scala.util.Try

case class SumDistinctStringWarning(tn: scala.meta.Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    """sumDistinct(columnName: String) is deprecated since Spark 3.2.0 and has no
      |sum_distinct(String) counterpart; rewrite it to sum_distinct(col(columnName)).""".stripMargin
}

/**
 * Renames the long-deprecated camelCase helpers in org.apache.spark.sql.functions to their
 * snake_case replacements. Matching is done on the resolved symbol (owner must be the
 * functions object), never on the bare name, so user-defined methods such as `def shiftLeft`
 * and `scala.math.toDegrees` are left alone.
 */
class FunctionsSnakeCaseRename extends SemanticRule("FunctionsSnakeCaseRename") {
  override val description =
    "Rename deprecated camelCase org.apache.spark.sql.functions.* helpers to their snake_case replacements."
  override val isRewrite = true

  // Replacements present since Spark 2.x.
  val sinceSpark2: Map[String, String] = Map(
    "approxCountDistinct" -> "approx_count_distinct",
    "monotonicallyIncreasingId" -> "monotonically_increasing_id",
    "toDegrees" -> "degrees",
    "toRadians" -> "radians"
  )

  // Replacements that only exist from Spark 3.2.0 onwards.
  val sinceSpark32: Map[String, String] = Map(
    "bitwiseNOT" -> "bitwise_not",
    "shiftLeft" -> "shiftleft",
    "shiftRight" -> "shiftright",
    "shiftRightUnsigned" -> "shiftrightunsigned",
    "callUDF" -> "call_udf",
    "sumDistinct" -> "sum_distinct"
  )

  val renames: Map[String, String] = sinceSpark2 ++ sinceSpark32

  val matchers: Map[String, SymbolMatcher] = renames.map { case (old, _) =>
    (old, SymbolMatcher.normalized("org.apache.spark.sql.functions." + old))
  }

  val functionsObject = SymbolMatcher.exact("org/apache/spark/sql/functions.")

  // The Column overload is declared first in functions.scala and the String overload second,
  // hence the (+1) disambiguator.
  val sumDistinctColumnOverload =
    SymbolMatcher.exact("org/apache/spark/sql/functions.sumDistinct().")
  val sumDistinctStringOverload =
    SymbolMatcher.exact("org/apache/spark/sql/functions.sumDistinct(+1).")

  private val stringTypes = Set("scala/Predef.String#", "java/lang/String#")

  private def firstParamIsString(sym: Symbol)(implicit doc: SemanticDocument): Boolean = {
    sym.info.map(_.signature) match {
      case Some(MethodSignature(_, List(List(param)), _)) =>
        param.signature match {
          case ValueSignature(TypeRef(_, tpeSym, _)) => stringTypes.contains(tpeSym.value)
          case _                                     => false
        }
      case _ => false
    }
  }

  private def isSumDistinctString(sym: Symbol)(implicit doc: SemanticDocument): Boolean = {
    if (!matchers("sumDistinct").matches(sym)) {
      false
    } else if (sumDistinctStringOverload.matches(sym)) {
      true
    } else if (sumDistinctColumnOverload.matches(sym)) {
      false
    } else {
      // Unexpected disambiguator: ask the classpath symbol table. That lookup can throw with an
      // old scalameta on a modern JDK (Scala 2.11 build on JDK 11), so treat failure as "not the
      // String overload" and fall back to a plain rename.
      Try(firstParamIsString(sym)).getOrElse(false)
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val utils = new Utils()
    val colImport = importer"org.apache.spark.sql.functions.col"

    def rename(name: Name): Patch = Patch.replaceTree(name, renames(name.value))

    // Rename the name token unless the call goes through an import alias
    // (`import functions.{sumDistinct => sd}`), whose importee is renamed separately.
    def renameIfDeprecatedName(name: Term.Name): Patch =
      if (name.value == "sumDistinct") rename(name) else Patch.empty

    // Original source text (Tree.syntax re-prints blocks across several lines).
    def source(tree: Tree): String = tree.pos.text

    def renamedFn(fn: Term, name: Term.Name): String =
      if (name.value == "sumDistinct") {
        fn match {
          case Term.Select(qual, _) => source(qual) + ".sum_distinct"
          case _                    => "sum_distinct"
        }
      } else {
        source(fn)
      }

    // The `col` helper has to be reachable: use the call's qualifier, or import it.
    def colAccess(fn: Term): (String, Patch) = fn match {
      case Term.Select(qual, _) => (source(qual) + ".col", Patch.empty)
      case _                    => ("col", utils.addImportIfNotPresent(colImport))
    }

    // A placeholder (`sumDistinct(_)`) or eta expansion of the String overload cannot be wrapped
    // in col(...). Renaming anything else in the file (the import in particular) would then
    // leave a mix that no longer compiles, so in that case every sumDistinct in the file is
    // frozen and only lints are emitted.
    def wrappable(name: Term.Name): Boolean = {
      val application = name.parent.flatMap {
        case select: Term.Select => select.parent
        case other               => Some(other)
      }
      application match {
        case Some(Term.Apply(_, List(_: Term.Placeholder))) => false
        case Some(Term.Apply(_, List(_)))                   => true
        case _                                              => false
      }
    }
    val freezeSumDistinct: Boolean = doc.tree.collect {
      case name: Term.Name if isSumDistinctString(name.symbol) && !wrappable(name) => name
    }.nonEmpty

    /**
     * sumDistinct("name") -> sum_distinct(col("name")): there is no sum_distinct(String), so the
     * String argument is wrapped. The argument shape decides how.
     */
    def rewriteSumDistinctString(apply: Term.Apply, fn: Term, name: Term.Name, arg: Term): Patch = {
      val (col, colPatch) = colAccess(fn)
      arg match {
        // sumDistinct(_) would turn into col(_): a function, not a Column. Leave it, but warn.
        case _: Term.Placeholder =>
          Patch.lint(SumDistinctStringWarning(name))
        // sumDistinct { name }: a wrapped block needs parentheses around the whole argument.
        case block: Term.Block =>
          Patch.replaceTree(apply, s"${renamedFn(fn, name)}($col(${source(block)}))") + colPatch
        // sumDistinct(columnName = name): sum_distinct's parameter is `e`, so drop the name.
        case Term.Assign(_, rhs) =>
          renameIfDeprecatedName(name) + Patch.replaceTree(arg, s"$col(${source(rhs)})") +
            colPatch + matchOnTree(rhs)
        case _ =>
          renameIfDeprecatedName(name) + Patch.addLeft(arg, col + "(") + Patch.addRight(arg, ")") +
            colPatch + matchOnTree(arg)
      }
    }

    // In a frozen file nothing is renamed; the uses that caused the freeze are reported.
    def frozen(name: Term.Name): Patch =
      if (wrappable(name)) Patch.empty else Patch.lint(SumDistinctStringWarning(name))

    def matchOnTree(tree: Tree): Patch = {
      tree match {
        case apply @ Term.Apply(fn @ Term.Select(_, name: Term.Name), List(arg))
            if isSumDistinctString(name.symbol) =>
          if (freezeSumDistinct) frozen(name) + matchOnTree(arg)
          else rewriteSumDistinctString(apply, fn, name, arg)
        case apply @ Term.Apply(name: Term.Name, List(arg)) if isSumDistinctString(name.symbol) =>
          if (freezeSumDistinct) frozen(name) + matchOnTree(arg)
          else rewriteSumDistinctString(apply, name, name, arg)
        // import org.apache.spark.sql.functions.{toDegrees, sumDistinct => sd}
        case Importer(ref, importees) if functionsObject.matches(ref) =>
          def renamable(name: Name): Boolean =
            renames.contains(name.value) && !(freezeSumDistinct && name.value == "sumDistinct")
          importees.collect {
            case Importee.Name(name) if renamable(name)      => rename(name)
            case Importee.Rename(name, _) if renamable(name) => rename(name)
          }.asPatch
        // Any other reference to the String overload (an eta expansion, possibly through an
        // import alias) cannot be wrapped: warn instead of breaking it.
        case name: Term.Name if isSumDistinctString(name.symbol) =>
          Patch.lint(SumDistinctStringWarning(name))
        // Bare `toDegrees(c)` or the name part of `functions.toDegrees(c)` / `F.toDegrees(c)`.
        // Aliased imports (`import functions.{toDegrees => deg}`) keep their alias untouched
        // because the name text no longer equals the deprecated name.
        case name: Term.Name
            if renames.contains(name.value) && matchers(name.value).matches(name) =>
          if (freezeSumDistinct && name.value == "sumDistinct") Patch.empty else rename(name)
        case elem =>
          elem.children match {
            case Nil => Patch.empty
            case _   => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}
