package fix

import scalafix.v1._
import scala.meta._

case class GroupByKeyWarning(tn: scala.meta.Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    """In Spark 2.4 and below,
      |Dataset.groupByKey results to a grouped dataset with key attribute is wrongly named as “value”,
      |if the key is non-struct type, for example, int, string, array, etc.
      |This is counterintuitive and makes the schema of aggregation queries unexpected.
      |For example, the schema of ds.groupByKey(...).count() is (value, count).
      |Since Spark 3.0, we name the grouping attribute to “key”.
      |The old behavior is preserved under a newly added configuration
      |spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue with a default value of false.
      |This linter rule is fuzzy.""".stripMargin
}

class GroupByKeyWarn extends SemanticRule("GroupByKeyWarn") {
  val matcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.groupByKey")
  override val description = "GroupByKey Warning."

  override def fix(implicit doc: SemanticDocument): Patch = {
    // Hacky.
    val grpByKey = "groupByKey"
    val funcToDS = "toDS"
    val agrFunCount = "count"

    if (doc.input.text.contains("groupByKey") && doc.input.text.contains("value") &&
      doc.input.text.contains("org.apache.spark.sql")) {
      doc.tree.collect {
        case matcher(gbk) =>
          Patch.lint(GroupByKeyWarning(gbk))
        case t @ Term.Apply(
            Term.Select(
              Term.Apply(
                Term.Select(
                  Term.Apply(Term.Select(_, _ @Term.Name(fName)), _),
                  gbk @ Term.Name(name)
                ),
                _
              ),
              _ @Term.Name(oprName)
            ),
            _
          )
          if grpByKey.equals(name) && funcToDS.equals(fName) && agrFunCount
            .equals(oprName) =>
          Patch.lint(GroupByKeyWarning(t))
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
