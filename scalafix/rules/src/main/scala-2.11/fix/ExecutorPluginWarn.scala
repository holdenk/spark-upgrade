package fix

import scalafix.v1._
import scala.meta._

class ExecutorPluginWarn extends SemanticRule("ExecutorPluginWarn") {
  // Executor plugin does not exist in early versions of Spark so skip the rule.

  override def fix(implicit doc: SemanticDocument): Patch = {
    None.asPatch
  }
}
