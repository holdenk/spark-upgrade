package fix

import scalafix.v1._
import scala.meta._

/**
 * Spark 4.0 removed five of the six spark.sql.legacy.*RebaseMode* keys; setting one of them to
 * any value other than CORRECTED throws at SparkSession creation / conf.set
 * (SQLConf.requireDefaultValueOfRemovedConf). The sixth,
 * spark.sql.legacy.parquet.datetimeRebaseModeInRead, is still accepted as an alternative name in
 * 4.0 and is renamed for consistency. The replacement keys exist from Spark 3.0.0 (datetime) /
 * 3.1.0 (int96), so this rewrite is safe to apply before upgrading.
 */
class RenamedSparkConfRewrite extends SyntacticRule("RenamedSparkConfRewrite") {
  override val description =
    "Rewrite removed spark.sql.legacy.*RebaseMode* config keys to their non-legacy names."
  override val isRewrite = true

  val renamedKeys: Map[String, String] = Map(
    "spark.sql.legacy.parquet.int96RebaseModeInWrite" -> "spark.sql.parquet.int96RebaseModeInWrite",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> "spark.sql.parquet.datetimeRebaseModeInWrite",
    "spark.sql.legacy.parquet.int96RebaseModeInRead" -> "spark.sql.parquet.int96RebaseModeInRead",
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead" -> "spark.sql.parquet.datetimeRebaseModeInRead",
    "spark.sql.legacy.avro.datetimeRebaseModeInWrite" -> "spark.sql.avro.datetimeRebaseModeInWrite",
    "spark.sql.legacy.avro.datetimeRebaseModeInRead" -> "spark.sql.avro.datetimeRebaseModeInRead"
  )

  override def fix(implicit doc: SyntacticDocument): Patch = {
    if (doc.input.text.contains("RebaseMode")) {
      doc.tree.collect {
        case lit @ Lit.String(value)
            if renamedKeys.contains(value) && !ConfLiterals.isPartialKey(lit) =>
          // Replace inside the original syntax so "..." and """...""" quoting is preserved.
          Patch.replaceTree(lit, lit.syntax.replace(value, renamedKeys(value)))
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
