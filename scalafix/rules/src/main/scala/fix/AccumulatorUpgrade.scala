package fix

import scalafix.v1._
import scala.meta._

case class AccMigrationGuide(position: Position) extends Diagnostic {
  val migrationGuide =
    "https://spark.apache.org/docs/latest/core-migration-guide.html#upgrading-from-core-24-to-30"
  def message = s"sc.accumulator is removed see ${migrationGuide}"
}

class AccumulatorUpgrade extends SemanticRule("AccumulatorUpgrade") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val accumulatorFunMatch = SymbolMatcher.normalized("org.apache.spark.SparkContext.accumulator")
    val utils = new Utils()

    def matchOnTree(e: Tree): Patch = {
      e match {
        // non-named accumulator
        case ns @ Term.Apply(j @ accumulatorFunMatch(f), params) =>
          // Find the spark context for rewriting
          val sc = ns.children(0).children(0)
          params match {
            case List(param) =>
              param match {
                // TODO: Handle non zero values
                case utils.intMatcher(initialValue) =>
                  Seq(
                    Patch.lint(AccMigrationGuide(e.pos)),
                    Patch.addLeft(e, "/*"),
                    Patch.addRight(e, "*/ null")).asPatch
                case q"0L" =>
                  Patch.replaceTree(ns, s"${sc}.longAccumulator")
                case utils.longMatcher(initialValue) =>
                  Patch.empty
                case q"0.0" =>
                  Patch.replaceTree(ns, s"${sc}.doubleAccumulator")
                case _ =>
                  Seq(
                    Patch.lint(AccMigrationGuide(e.pos)),
                    Patch.addLeft(e, "/*"),
                    Patch.addRight(e, "*/ null")).asPatch
              }
            case List(param, name) =>
              Seq(
                Patch.lint(AccMigrationGuide(e.pos)),
                Patch.addLeft(e, "/*"),
                Patch.addRight(e, "*/ null")).asPatch
          }
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
