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
    // Match the name token only so `TaskContext.get().isRunningLocally()` is reported once
    // rather than once for each enclosing Select/Apply node that carries the same symbol.
    doc.tree.collect {
      case name: Term.Name if matcher.matches(name) =>
        Patch.lint(IsRunningLocally(name))
    }.asPatch
  }
}
