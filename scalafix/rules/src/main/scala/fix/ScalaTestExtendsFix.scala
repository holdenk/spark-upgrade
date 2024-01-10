package fix

import scalafix.v1._
import scala.meta._

// Fix the with with since the QQ matcher doesn't like it and I'm lazy.
class fix.fix.ScalaTestExtendsFix
    extends SyntacticRule("ScalaTestExtendsFix") with Rule {
  override val description =
    """Handle the change with ScalaTest ( see https://www.scalatest.org/release_notes/3.1.0 ) """

  override val isRewrite = true

  override def fix(implicit doc: SyntacticDocument): Patch = {
    println("Magicz!")
    doc.tree.collect {
      case v: Type.Name if v.toString == "FunSuite" || v.toString == "FunSpec" =>
        Patch.replaceTree(v, "AnyFunSuite")
    case v: Type.Name if v.toString == "FlatSpec" =>
        Patch.replaceTree(v, "AnyFlatSpec")
    case v: Type.Name if v.toString == "FreeSpec" =>
        Patch.replaceTree(v, "AnyFreeSpec")
    case v: Type.Name if v.toString == "PropSpec" =>
        Patch.replaceTree(v, "AnyPropSpec")
    case v: Type.Name if v.toString == "FeatureSpec" =>
        Patch.replaceTree(v, "AnyFeatureSpec")
}
