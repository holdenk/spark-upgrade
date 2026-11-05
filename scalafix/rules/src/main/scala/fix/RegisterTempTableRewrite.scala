package fix

import scalafix.v1._
import scala.meta._

/**
 * Dataset.registerTempTable(name) has been a one-line forwarder to
 * createOrReplaceTempView(name) since Spark 2.0 (deprecated since 2.0.0, still deprecated
 * in 4.0.0). Rename the method on the resolved Dataset symbol only, so user-defined
 * methods that happen to be called registerTempTable are untouched.
 */
class RegisterTempTableRewrite extends SemanticRule("RegisterTempTableRewrite") {
  override val description =
    "Rewrite Dataset.registerTempTable(name) to createOrReplaceTempView(name)."
  override val isRewrite = true

  val registerTempTable = SymbolMatcher.exact("org/apache/spark/sql/Dataset#registerTempTable().")

  override def fix(implicit doc: SemanticDocument): Patch = {
    if (doc.input.text.contains("registerTempTable")) {
      // Matching the name token covers `df.registerTempTable("t")`, the infix form
      // `df registerTempTable "t"` and eta expansions; the signature is identical so
      // only the name needs to change.
      doc.tree.collect {
        case name @ Term.Name("registerTempTable") if registerTempTable.matches(name) =>
          Patch.replaceTree(name, "createOrReplaceTempView")
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
