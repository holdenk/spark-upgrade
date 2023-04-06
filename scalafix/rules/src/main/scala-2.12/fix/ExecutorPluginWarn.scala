package fix

import scalafix.v1._
import scala.meta._

case class ExecutorPluginWarning(e: scala.meta.Tree) extends Diagnostic {
  override def position: Position = e.pos
  override def message: String =
    "Executor Plugin is dropped in 3.0+, see " +
  "https://spark.apache.org/docs/3.0.0/core-migration-guide.html " +
  " https://spark.apache.org/docs/3.2.1/api/java/index.html?org/apache/spark/api/plugin/SparkPlugin.html"
}

class ExecutorPluginWarn extends SemanticRule("ExecutorPluginWarn") {
  // See https://spark.apache.org/docs/3.0.0/core-migration-guide.html +
  // + new docs at:
  // https://spark.apache.org/docs/3.2.1/api/java/index.html?org/apache/spark/api/plugin/SparkPlugin.html
  // https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/api/plugin/ExecutorPlugin.html

  val matcher = SymbolMatcher.normalized("org.apache.spark.ExecutorPlugin")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case matcher(s)  =>
        Patch.lint(ExecutorPluginWarning(s))
    }.asPatch
  }
}
