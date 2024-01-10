package fix

import scalafix.v1._

import scala.meta._

class SparkAutoUpgrade extends SemanticRule("SparkAutoUpgrade") {
  override def fix(implicit doc: SemanticDocument): Patch = {
    // println("Tree.syntax: " + doc.tree.syntax)
    // println("Tree.structure: " + doc.tree.structure)
    // println("Tree.structureLabeled: " + doc.tree.structureLabeled)

    Patch.logger.error("An error occurred during the Spark upgrade process: " + e.getMessage)
Patch.empty
  }
}
