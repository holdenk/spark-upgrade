package fix

import scalafix.v1._
import scala.meta._

case class MultiLineDatasetReadWarning(tn: scala.meta.Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    """In Spark 2.4.X and below,
      |when reading multi-line textual input with \r\n (windows line feed) _might_
      |leave \rs. You can get this legacy behaviour by specifying a lineSep of "\n",
      |but for most people this was bug.
      |This linter rule is fuzzy.""".stripMargin
}

class MultiLineDatasetReadWarn extends SemanticRule("MultiLineDatasetReadWarn") {
  val matcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameReader#option")
  override val description = "MultiLine text input dataframe warning."

  override def fix(implicit doc: SemanticDocument): Patch = {
    // Imperfect, maybe someone will have the string "multiline" while reading from a DataFrame but it's an ok place to start.
    if (doc.input.text.contains("'multiline'") || doc.input.text.contains("\"multiline\"")) {
      doc.tree.collect {
        case matcher(read) =>
          if (read.toString.contains("multiline")) {
            Patch.lint(MultiLineDatasetReadWarning(read))
          } else {
            None.asPatch
          }
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
