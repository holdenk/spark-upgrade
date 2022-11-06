package fix

import metaconfig.ConfDecoder.canBuildFromAnyMapWithStringKey
import metaconfig.generic.Surface
import metaconfig.{ConfDecoder, Configured}
import scalafix.v1._

import scala.meta._
final case class SparkAutoUpgradeConfig(
  deprecatedMethod: Map[String, String]
)

object SparkAutoUpgradeConfig {
  val default: SparkAutoUpgradeConfig =
    SparkAutoUpgradeConfig(
      deprecatedMethod = Map(
        "unionAll" -> "union"
      )
    )

  implicit val surface: Surface[SparkAutoUpgradeConfig] =
    metaconfig.generic.deriveSurface[SparkAutoUpgradeConfig]
  implicit val decoder: ConfDecoder[SparkAutoUpgradeConfig] =
    metaconfig.generic.deriveDecoder(default)
}

class SparkAutoUpgrade(config: SparkAutoUpgradeConfig) extends SemanticRule("SparkAutoUpgrade") {
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

    def matchOnTree(t: Tree): Patch = {
      t.collect {
        case Term.Apply(
            Term.Select(_, deprecated @ Term.Name(name)),
            _
            ) if config.deprecatedMethod.contains(name) =>
          Patch.replaceTree(
            deprecated,
            config.deprecatedMethod(name)
          )
        case Term.Apply(
            Term.Select(_, _ @Term.Name(name)),
            List(
              Term.AnonymousFunction(
                Term.ApplyInfix(
                  _,
                  deprecatedAnm @ Term.Name(nameAnm),
                  _,
                  _
                )
              )
            )
            ) if "reduce".contains(name) && config.deprecatedMethod.contains(nameAnm) =>
          Patch.replaceTree(
            deprecatedAnm,
            config.deprecatedMethod(nameAnm)
          )
      }.asPatch
    }

    matchOnTree(doc.tree)
  }
}
