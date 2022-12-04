package fix

import scalafix.v1._
import scala.meta._

class MigrateTrigger extends SemanticRule("MigrateTrigger") {
  override val description =
    """Migrate Trigger."""
  override val isRewrite = true

  val triggerMatcher = SymbolMatcher.normalized("org.apache.spark.sql.streaming.ProcessingTime")

  override def fix(implicit doc: SemanticDocument): Patch = {
    val utils = new Utils()
    def matchOnTree(e: Tree): Patch = {
      e match {
        case triggerMatcher =>
          utils.addImportIfNotPresent(importer"org.apache.spark.sql.streaming.Trigger._")
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}
