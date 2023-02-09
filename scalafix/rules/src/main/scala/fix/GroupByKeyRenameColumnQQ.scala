package fix

import scalafix.v1._
import scala.meta._

class GroupByKeyRenameColumnQQ
    extends SemanticRule("GroupByKeyRenameColumnQQ") {
  override val description =
    """Renaming column "value" with "key" when have Dataset.groupByKey(...).count()"""

  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {

    def matchOnTerm(t: Term): Patch = {
      t match {
        case q""""value"""" => Patch.replaceTree(t, q""""key"""".toString())
        case q"""'value"""  => Patch.replaceTree(t, q"""'key""".toString())
        case q"""col("value")""" =>
          Patch.replaceTree(t, q"""col("key")""".toString())
        case q"""upper(col("value"))""" =>
          Patch.replaceTree(t, q"""upper(col("key"))""".toString())
        case q"""upper(col('value))""" =>
          Patch.replaceTree(t, q"""upper(col('key))""".toString())
        case _ => Patch.empty
      }
    }

    val dsGBKmatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.groupByKey")
    val dsMatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset")
    val dfMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrame")

    def isDSGroupByKey(t: Term): Boolean = {
      val isDataset = t.collect {
        case q"""Dataset""" => true
        case dsGBKmatcher(_) => true
        case dfMatcher(_) => true
        case dsMatcher(_) => true
      }
      val isGroupByKey = t.collect { case q"""groupByKey""" => true }
      (isGroupByKey.isEmpty.equals(false) && isGroupByKey.head.equals(
        true
      )) && (isDataset.isEmpty.equals(false) && isDataset.head.equals(
        true
      ))

    }

    def matchOnTree(t: Tree): Patch = {
      t match {
        case _ @Term.Apply(tr, params) =>
          if (isDSGroupByKey(tr)) params.map(matchOnTerm).asPatch
          else Patch.empty
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ =>
              elem.children.map(matchOnTree).asPatch
          }
      }
    }

    // Bit of a hack, but limit our blast radius
    if (doc.input.text.contains("groupByKey") && doc.input.text.contains("value") &&
      doc.input.text.contains("org.apache.spark.sql")) {
      matchOnTree(doc.tree)
    } else {
      Patch.empty
    }
  }
}
