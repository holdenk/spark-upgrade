package fix

import scalafix.v1._
import scala.meta._

class UnionRewriteQQ extends SemanticRule("UnionRewriteQQ") {
  override val description =
    """Replacing unionAll with union. Quasiquotes."""
  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {

    def matchOnTree(t: Tree): Patch = {
      t.collect { case tt: Term =>
        tt match {
          case q"""unionAll""" =>
            Patch.replaceTree(tt, q"""union""".toString())
          case _ => Patch.empty
        }
      }.asPatch
    }

    matchOnTree(doc.tree)
  }

}
