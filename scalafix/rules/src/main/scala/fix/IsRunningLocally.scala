package fix

import scalafix.v1._
import scala.meta._

case class IsRunningLocally(e: scala.meta.Tree) extends Diagnostic {
  override def position: Position = e.pos
  override def message: String =
    "TaskContext.isRunningLocally has been removed, see " +
  "https://spark.apache.org/docs/3.0.0/core-migration-guide.html " +
  " since local execution was removed you can probably delete this code path."
}

class IsRunningLocallyWarn extends SemanticRule("IsRunningLocallyWarn") {

  val matcher = SymbolMatcher.normalized("org.apache.spark.TaskContext.isRunningLocally")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case matcher(s)  =>
        Patch.lint(IsRunningLocally(s))
    }.asPatch
  }
}
