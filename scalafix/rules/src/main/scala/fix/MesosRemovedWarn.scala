package fix

import scalafix.v1._
import scala.meta._

case class MesosRemovedWarning(tn: scala.meta.Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    """Apache Mesos support as a resource manager was removed in Spark 4.0.
      |Migrate this application to a supported cluster manager such as YARN,
      |Kubernetes, or Spark Standalone before upgrading.
      |This linter rule is fuzzy.""".stripMargin
}

class MesosRemovedWarn extends SemanticRule("MesosRemovedWarn") {
  override val description =
    "Warn about removed Apache Mesos support in Spark 4.0."

  override def fix(implicit doc: SemanticDocument): Patch = {
    if (doc.input.text.contains("mesos://")) {
      doc.tree.collect {
        case lit @ Lit.String(value) if value.startsWith("mesos://") =>
          Patch.lint(MesosRemovedWarning(lit))
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
