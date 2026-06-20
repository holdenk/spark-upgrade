package fix

import scalafix.v1._
import scala.meta._

case class AnsiModeEnabledWarning(tn: scala.meta.Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    """Since Spark 4.0, ANSI SQL mode (spark.sql.ansi.enabled) is enabled by default.
      |Operations that previously returned NULL on error - such as numeric overflow on
      |cast, division by zero, and invalid date/number parsing - may now throw exceptions.
      |Review casts and arithmetic, or set spark.sql.ansi.enabled to false to retain the
      |pre-4.0 behavior.
      |This linter rule is fuzzy.""".stripMargin
}

class AnsiModeEnabledWarn extends SemanticRule("AnsiModeEnabledWarn") {
  override val description =
    "Warn that ANSI SQL mode is enabled by default starting in Spark 4.0."

  override def fix(implicit doc: SemanticDocument): Patch = {
    // Attach a single hint where the SparkSession is built to avoid flagging every cast.
    if (doc.input.text.contains("SparkSession")) {
      doc.tree.collect {
        case t @ Term.Select(Term.Name("SparkSession"), Term.Name("builder")) =>
          Patch.lint(AnsiModeEnabledWarning(t))
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
