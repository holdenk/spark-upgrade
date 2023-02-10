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
        case q"""col("value").as""" =>
          Patch.replaceTree(t, q"""col("key").as""".toString())
        case q"""col("value").alias""" =>
          Patch.replaceTree(t, q"""col("key").alias""".toString())
        case q"""upper(col("value"))""" =>
          Patch.replaceTree(t, q"""upper(col("key"))""".toString())
        case q"""upper(col('value))""" =>
          Patch.replaceTree(t, q"""upper(col('key))""".toString())
        case _ if ! t.children.isEmpty =>
          t.children.map {
            case e: scala.meta.Term => matchOnTerm(e)
            case _ => Patch.empty
          }.asPatch
        case _ => Patch.empty
      }
    }

    val dsGBKmatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.groupByKey")
    val dsSelect = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.select")
    val dsMatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset")
    val dfMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrame")
    val keyedDs = SymbolMatcher.normalized("org.apache.spark.sql.KeyValueGroupedDataset")
    val keyedDsCount = SymbolMatcher.normalized("org.apache.spark.sql.KeyValueGroupedDataset.count")

    def isDSGroupByKey(t: Term): Boolean = {
      val isDataset = t.collect {
        case q"""DataFame""" => true
        case q"""Dataset""" => true
        case q"""Dataset[_]""" => true
        case dsGBKmatcher(_) => true
        case dfMatcher(_) => true
        case dsMatcher(_) => true
        case dsSelect(_) => true
        case keyedDs(_) => true
        case keyedDsCount(_) => true
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
        case _ @Term.Apply(tr, params) if (isDSGroupByKey(tr)) =>
          List(
            params.map(matchOnTerm).asPatch,
            params.map(matchOnTree).asPatch,
            tr.children.map(matchOnTree).asPatch,
          ).asPatch
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
