package fix

import scalafix.v1._
import scala.meta._

// Fix the extends with since the QQ matcher doesn't like it and I'm lazy.
class UpdatedClassName
    extends SyntacticRule("UpdatedClassName") {
  override val description =
    """Handle the change with ScalaTest ( see https://www.scalatest.org/release_notes/3.1.0 ) """

  override val isRewrite = true

  override def fix(implicit doc: SyntacticDocument): Patch = {
    println("Magicz!")
    doc.tree.collect { case v: Type.Name =>
      println(v)
      if (v.toString == "DesiredClassName") {
        Patch.replaceTree(v, "UpdatedClassName")
      } else {
        println(s"No change to $v")
        Patch.empty
      }
    }.asPatch
  }
}
