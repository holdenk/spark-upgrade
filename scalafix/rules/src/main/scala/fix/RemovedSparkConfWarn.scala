package fix

import scalafix.v1._
import scala.meta._

/** Shared by the config-key rules: literals that are only a fragment of a key. */
object ConfLiterals {

  /**
   * True when the literal is a piece of an interpolation with arguments
   * (s"spark.blacklist.$x") or an operand of string concatenation ("spark.blacklist." + x): the
   * literal is then only a prefix/suffix of the real key, so it must not be matched or rewritten.
   * A plain s"..." without arguments is the whole key and is treated like an ordinary literal.
   */
  def isPartialKey(lit: Lit): Boolean = {
    lit.parent match {
      case Some(Term.Interpolate(_, _, args))             => args.nonEmpty
      case Some(Term.ApplyInfix(_, Term.Name("+"), _, _)) => true
      case _                                              => false
    }
  }
}

case class RemovedSparkConfWarning(tn: scala.meta.Tree, text: String) extends Diagnostic {
  override def position: Position = tn.pos
  override def message: String = text
}

/**
 * Warns about removed or renamed Spark configuration keys passed as string literals: the
 * *.blacklist.* family (ignored since Spark 4.1), the YARN failure configs, the shuffle
 * buffer key, the launcher typo and spark.executor.plugins. Exact keys first, then a fuzzy
 * fallback for anything that looks like a spark.*.blacklist.* key.
 */
class RemovedSparkConfWarn extends SyntacticRule("RemovedSparkConfWarn") {
  override val description =
    "Warn on removed or renamed Spark config keys (blacklist, yarn failures, shuffle buffer, ...)."

  val coreGuide =
    "https://spark.apache.org/docs/latest/core-migration-guide.html#upgrading-from-core-40-to-41"

  val excludeOnFailureRenames: Map[String, String] = Map(
    "spark.blacklist.enabled" -> "spark.excludeOnFailure.enabled",
    "spark.blacklist.task.maxTaskAttemptsPerExecutor" -> "spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor",
    "spark.blacklist.task.maxTaskAttemptsPerNode" -> "spark.excludeOnFailure.task.maxTaskAttemptsPerNode",
    "spark.blacklist.application.maxFailedTasksPerExecutor" -> "spark.excludeOnFailure.application.maxFailedTasksPerExecutor",
    "spark.blacklist.stage.maxFailedTasksPerExecutor" -> "spark.excludeOnFailure.stage.maxFailedTasksPerExecutor",
    "spark.blacklist.application.maxFailedExecutorsPerNode" -> "spark.excludeOnFailure.application.maxFailedExecutorsPerNode",
    "spark.blacklist.stage.maxFailedExecutorsPerNode" -> "spark.excludeOnFailure.stage.maxFailedExecutorsPerNode",
    "spark.blacklist.timeout" -> "spark.excludeOnFailure.timeout",
    "spark.blacklist.application.fetchFailure.enabled" -> "spark.excludeOnFailure.application.fetchFailure.enabled",
    "spark.blacklist.killBlacklistedExecutors" -> "spark.excludeOnFailure.killExcludedExecutors",
    "spark.scheduler.blacklist.unschedulableTaskSetTimeout" -> "spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout",
    "spark.yarn.blacklist.executor.launch.blacklisting.enabled" -> "spark.yarn.executor.launch.excludeOnFailure.enabled"
  )

  private def blacklistMessage(old: String, replacement: String): String =
    s"$old is deprecated since Spark 3.1.0 and silently ignored since Spark 4.1 " +
      s"(Spark 4.1 ignores *.blacklist.* configuration names); use $replacement instead " +
      s"(available since Spark 3.1.0). See $coreGuide"

  private def stillWorksMessage(old: String, since: String, replacement: String): String =
    s"$old is deprecated since Spark $since and still works but will be removed; " +
      s"use $replacement instead (available since Spark $since). See $coreGuide"

  val messages: Map[String, String] = excludeOnFailureRenames.map { case (old, replacement) =>
    (old, blacklistMessage(old, replacement))
  } ++ Map(
    "spark.scheduler.executorTaskBlacklistTime" ->
      ("spark.scheduler.executorTaskBlacklistTime is deprecated and ignored since Spark 2.1.0; " +
        "use the spark.excludeOnFailure.* configs (available since Spark 3.1.0) instead. " +
        s"See $coreGuide"),
    "spark.shuffle.unsafe.file.output.buffer" ->
      ("spark.shuffle.unsafe.file.output.buffer is deprecated since Spark 4.0 (still works). " +
        "Once the job runs on Spark 4.x only, switch to spark.shuffle.localDisk.file.output.buffer " +
        s"(the new key does not exist on Spark 3.x). See $coreGuide"),
    "spark.yarn.max.executor.failures" ->
      stillWorksMessage(
        "spark.yarn.max.executor.failures",
        "3.5.0",
        "spark.executor.maxNumFailures"
      ),
    "spark.yarn.executor.failuresValidityInterval" ->
      stillWorksMessage(
        "spark.yarn.executor.failuresValidityInterval",
        "3.5.0",
        "spark.executor.failuresValidityInterval"
      ),
    "spark.launcher.childConectionTimeout" ->
      stillWorksMessage(
        "spark.launcher.childConectionTimeout",
        "3.2.0",
        "spark.launcher.childConnectionTimeout"
      ),
    "spark.executor.plugins" ->
      ("spark.executor.plugins was removed in Spark 3.0 and is ignored; use spark.plugins with " +
        "an org.apache.spark.api.plugin.SparkPlugin implementation instead. See " +
        "https://spark.apache.org/docs/latest/core-migration-guide.html#upgrading-from-core-24-to-30"),
    "spark.sql.hive.verifyPartitionPath" ->
      ("spark.sql.hive.verifyPartitionPath was removed in Spark 4.0; setting it to true throws. " +
        "Use spark.files.ignoreMissingFiles instead and review that the semantics (ignore " +
        "missing files globally) are acceptable. See " +
        "https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-35-to-40")
  )

  private def fuzzyMessage(key: String): String =
    s"$key looks like a *.blacklist.* configuration name, which Spark 4.1 silently ignores; " +
      "use the corresponding spark.excludeOnFailure.* name (available since Spark 3.1.0). " +
      s"See $coreGuide\nThis linter rule is fuzzy."

  private def looksLikeBlacklistKey(value: String): Boolean =
    value.startsWith("spark.") && (value.contains(".blacklist.") || value.endsWith(".blacklist"))

  override def fix(implicit doc: SyntacticDocument): Patch = {
    val text = doc.input.text
    val mayMatch = text.contains("blacklist") || messages.keys.exists(key => text.contains(key))
    if (mayMatch) {
      doc.tree.collect {
        case lit @ Lit.String(value)
            if messages.contains(value) && !ConfLiterals.isPartialKey(lit) =>
          Patch.lint(RemovedSparkConfWarning(lit, messages(value)))
        case lit @ Lit.String(value)
            if looksLikeBlacklistKey(value) && !value.endsWith(".") &&
              !ConfLiterals.isPartialKey(lit) =>
          Patch.lint(RemovedSparkConfWarning(lit, fuzzyMessage(value)))
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
