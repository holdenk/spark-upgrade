package fix

import scalafix.v1._
import scala.meta._

class SparkAutoUpgrade extends SemanticRule("SparkAutoUpgrade") {
  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {
    println("Tree.syntax: " + doc.tree.syntax)
    println("Tree.structure: " + doc.tree.structure)
    println("Tree.structureLabeled: " + doc.tree.structureLabeled)
    replaceTerm("unionAll", "union")
  }

  def replaceTerm(oldValue: String, newValue: String)(implicit
                                                      doc: SemanticDocument
  ): Patch = {
    doc.tree.collect {
      case Term.Apply(
      Term.Select(_, t@Term.Name(_)),
      _ :: Nil
      ) if t.toString() == oldValue =>
        Patch.replaceTree(
          t,
          newValue
        )
      case Term.Apply(
      Term.Select(_, f@Term.Name(_)),
      List(
      Term.AnonymousFunction(
      Term.ApplyInfix(
      _,
      n@Term.Name(_),
      _,
      _
      )
      )
      )
      ) if f.toString() == "reduce" && n.toString() == oldValue =>
        Patch.replaceTree(
          n,
          newValue
        )

    }.asPatch
  }

}
