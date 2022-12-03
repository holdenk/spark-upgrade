package fix

import scalafix.v1._
import scala.meta._

class ScalaTestImportChange
    extends SemanticRule("ScalaTestImportChange") {
  override val description =
    """Handle the import change with ScalaTest ( see https://www.scalatest.org/release_notes/3.1.0 ) """

  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {

    def matchOnTree(t: Tree): Patch = {
      t match {
        case q"""import org.scalatest.FunSuite""" =>
          Patch.replaceTree(t, q"""import org.scalatest.funsuite.AnyFunSuite""".toString())
        case q"""class $cls extends FunSuite { $expr }""" =>
          Patch.replaceTree(t, f"class $cls extends AnyFunSuite { $expr }")
        case q"""import org.scalatest.FunSuiteLike""" =>
          Patch.replaceTree(t, q"""import org.scalatest.funsuite.AnyFunSuiteLike""".toString())
        case q"""class $cls extends FunSuiteLike { $expr }""" =>
          Patch.replaceTree(t, q"class $cls extends AnyFunSuiteLike { $expr }".toString)
        case q"""import org.scalatest.AsyncFunSuite""" =>
          Patch.replaceTree(t, q"""import org.scalatest.funsuite.AsyncFunSuiteLike""".toString())
        case q"""import org.scalatest.fixture.FunSuite""" =>
          Patch.replaceTree(t, q"""import org.scalatest.funsuite.FixtureAnyFunSuite""".toString())
        case q"""import org.scalatest.Matchers._""" =>
          Patch.replaceTree(t, q"""import org.scalatest.matchers.should.Matchers._""".toString())
        case q"""import org.scalatest.Matchers""" =>
          Patch.replaceTree(t, q"""import org.scalatest.matchers.should.Matchers""".toString())
        case q"""import org.scalatest.MustMatchers""" =>
          Patch.replaceTree(t, """import org.scalatest.matchers.must.{Matchers => MustMatchers}""")
        case q"""import org.scalatest.MustMatchers._""" =>
          Patch.replaceTree(t, """import org.scalatest.matchers.must.Matchers._""")
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ =>
              elem.children.map(matchOnTree).asPatch
          }
      }
    }

    matchOnTree(doc.tree)
  }
}
