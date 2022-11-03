package fix

import metaconfig.ConfDecoder.canBuildFromAnyMapWithStringKey
import metaconfig.generic.Surface
import metaconfig.ConfDecoder
import metaconfig.Configured
import scalafix.v1._
import scala.meta._
final case class SparkAutoUpgradeConfig(
    depricatedMethod: Map[String, String] = Map("unionAll" -> "union")
)

object SparkAutoUpgradeConfig {
  def default: SparkAutoUpgradeConfig =
    SparkAutoUpgradeConfig(depricatedMethod =
      Map(
        "unionAll" -> "union"
      )
    )
  implicit val surface: Surface[SparkAutoUpgradeConfig] =
    metaconfig.generic.deriveSurface[SparkAutoUpgradeConfig]
  implicit val decoder: ConfDecoder[SparkAutoUpgradeConfig] =
    metaconfig.generic.deriveDecoder(default)

}

class SparkAutoUpgrade(config: SparkAutoUpgradeConfig)
    extends SemanticRule("SparkAutoUpgrade") {
  def this() = this(SparkAutoUpgradeConfig.default)

  override def withConfiguration(config: Configuration): Configured[Rule] =
    config.conf.getOrElse("SparkAutoUpgrade")(this.config).map { newConfig =>
      new SparkAutoUpgrade(newConfig)
    }

  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {
    println("Tree.syntax: " + doc.tree.syntax)
    println("Tree.structure: " + doc.tree.structure)
    println("Tree.structureLabeled: " + doc.tree.structureLabeled)

    // TODO Check with list depricated method
    config.depricatedMethod.map { m => replaceTerm(m._1, m._2) }.asPatch
  }

  def replaceTerm(oldValue: String, newValue: String)(implicit
      doc: SemanticDocument
  ): Patch = {
    doc.tree.collect {
      case Term.Apply(
            Term.Select(_, t @ Term.Name(_)),
            _ :: Nil
          ) if t.toString() == oldValue =>
        Patch.replaceTree(
          t,
          newValue
        )
      case Term.Apply(
            Term.Select(_, f @ Term.Name(_)),
            List(
              Term.AnonymousFunction(
                Term.ApplyInfix(
                  _,
                  n @ Term.Name(_),
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
