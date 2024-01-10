package fix

import scalafix.v1._
import scala.meta._

class AllEquivalentExprs extends SemanticRule("AllEquivalentExprs") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val equivExprs = SymbolMatcher.normalized("org/apache/spark/sql/catalyst/expressions/EquivalentExpressions#getAllEquivalentExprs().")
    val utils = new Utils()

    def matchOnTree(e: Tree): Patch = {
      e match {
        case equivExprs(call) =>
          // This is sketch because were messing with the string repr but it's easier
          // since we only want to replace some of our match.
          val newCall = call.toString.replace(".getAllEquivalentExprs()", ".getCommonSubexpressions.map(List(_))")
          Patch.replaceTree(call, newCall)
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
