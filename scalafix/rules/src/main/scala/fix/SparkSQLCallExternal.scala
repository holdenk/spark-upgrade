package fix

import scalafix.v1._
import scala.meta._

class SparkSQLCallExternal extends SemanticRule("SparkSQLCallExternal") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val sparkSQLFunMatch = SymbolMatcher.normalized("org.apache.spark.sql.SparkSession.sql")
    val utils = new Utils()

    def matchOnTree(e: Tree): Patch = {
      e match {
        // non-named accumulator
        case ns @ Term.Apply(j @ sparkSQLFunMatch(f), params) =>
          // Find the spark context for rewriting
          params match {
            case List(param) =>
              param match {
                case Lit.String(_) =>
                  Patch.replaceTree(param, "\"magic\"")
                case _ =>
                  Patch.empty
              }
            case _ =>
              Patch.empty
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
