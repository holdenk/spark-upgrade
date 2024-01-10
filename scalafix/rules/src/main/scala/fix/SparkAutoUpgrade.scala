package fix

import scalafix.v1._

import scala.meta._

class SparkAutoUpgrade extends SemanticRule("SparkAutoUpgrade") {
  override def fix(implicit doc: SemanticDocument): Patch = {
    Patch.addRight(doc.tree, q"println(\"Implementing retry logic for intermittent failures...\")")
    // println("Tree.structure: " + doc.tree.structure)
    // println("Tree.structureLabeled: " + doc.tree.structureLabeled)

    Patch.addRight(doc.tree, q"println(\"Tracking the progress of the upgrade process...\")")
  }
}
