package fix
import metaconfig.generic.Surface
import metaconfig.{ConfDecoder, Configured}
import scalafix.v1._

import scala.meta._
final case class UnionRewriteConfig(
  deprecatedMethod: Map[String, String]
)

object UnionRewriteConfig {
  val default: UnionRewriteConfig =
    UnionRewriteConfig(
      deprecatedMethod = Map(
        "unionAll" -> "union"
      )
    )

  implicit val surface: Surface[UnionRewriteConfig] =
    metaconfig.generic.deriveSurface[UnionRewriteConfig]
  implicit val decoder: ConfDecoder[UnionRewriteConfig] =
    metaconfig.generic.deriveDecoder(default)
}

class UnionRewrite(config: UnionRewriteConfig) extends SemanticRule("UnionRewrite") {
  def this() = this(UnionRewriteConfig.default)

  override def withConfiguration(config: Configuration): Configured[Rule] =
    config.conf.getOrElse("UnionRewrite")(this.config).map { newConfig =>
      new UnionRewrite(newConfig)
    }

  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {
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
