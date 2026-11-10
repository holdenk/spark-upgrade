package fix

import scalafix.v1._
import scala.meta._
import scala.collection.mutable.ListBuffer

case class DateTimePatternWarning(
    tn: scala.meta.Tree,
    pattern: String,
    parsing: Boolean,
    issues: List[String],
    suggestions: List[String]
) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String = {
    val kind = if (parsing) "parsing" else "formatting"
    "Spark 3.0 uses java.time DateTimeFormatter patterns with strict checking (SQL migration " +
      s"""guide, 2.4 -> 3.0). The $kind pattern "$pattern" ${issues.mkString("; ")}. """ +
      s"Suggested: ${suggestions.mkString("; ")}. To keep the Spark 2.4 behaviour globally set " +
      "spark.sql.legacy.timeParserPolicy=LEGACY (see " +
      "https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)."
  }
}

/**
 * Inspects string-literal datetime patterns handed to functions.date_format / from_unixtime
 * (formatting), functions.to_date / to_timestamp / unix_timestamp (parsing) and the
 * dateFormat / timestampFormat options of the DataFrame and streaming readers (parsing) and
 * writers (formatting), and flags the SimpleDateFormat letters that are rejected or changed
 * meaning under the Spark 3.0 DateTimeFormatter. Only plain literals are inspected; quoted
 * text ('T', '' ...) inside the pattern is skipped.
 */
class DateTimePatternWarn extends SemanticRule("DateTimePatternWarn") {
  override val description =
    "Warn on datetime pattern letters that are unsupported or changed meaning in Spark 3.0."

  val formattingFunctions = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.date_format",
    "org.apache.spark.sql.functions.from_unixtime"
  )
  val parsingFunctions = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.to_date",
    "org.apache.spark.sql.functions.to_timestamp",
    "org.apache.spark.sql.functions.unix_timestamp"
  )
  val parsingOptions = SymbolMatcher.normalized(
    "org.apache.spark.sql.DataFrameReader.option",
    "org.apache.spark.sql.streaming.DataStreamReader.option"
  )
  val formattingOptions = SymbolMatcher.normalized(
    "org.apache.spark.sql.DataFrameWriter.option",
    "org.apache.spark.sql.streaming.DataStreamWriter.option"
  )
  val patternOptionKeys = Set("dateFormat", "timestampFormat")

  private val weekBasedLetters = "YWwuec"
  private val illegalLetters = "ABnNp"
  private val parsingOnlyLetters = "EFqQ"
  private val lengthLimitedLetters = "GMLEQq"

  /** Drops single-quoted sections, treating '' as an escaped quote. */
  def stripQuotedText(pattern: String): String = {
    val out = new StringBuilder
    var inQuote = false
    var i = 0
    while (i < pattern.length) {
      val c = pattern.charAt(i)
      if (c == '\'') {
        if (i + 1 < pattern.length && pattern.charAt(i + 1) == '\'') {
          i += 1 // escaped quote, not a letter: skip both
        } else {
          inQuote = !inQuote
        }
      } else if (!inQuote) {
        out.append(c)
      }
      i += 1
    }
    out.toString
  }

  /** Runs of identical letters as (letter, run length), in order of appearance. */
  def letterRuns(text: String): List[(Char, Int)] = {
    val runs = ListBuffer[(Char, Int)]()
    var i = 0
    while (i < text.length) {
      val c = text.charAt(i)
      var j = i
      while (j < text.length && text.charAt(j) == c) j += 1
      if (c.isLetter) runs += ((c, j - i))
      i = j
    }
    runs.toList
  }

  private def quoteLetters(letters: Seq[Char]): String =
    letters.distinct.map(c => s"'$c'").mkString(", ")

  def check(lit: Lit.String, pattern: String, parsing: Boolean): Patch = {
    val runs = letterRuns(stripQuotedText(pattern))
    val issues = ListBuffer[String]()
    val suggestions = ListBuffer[String]()

    val weekBased = runs.map(_._1).filter(c => weekBasedLetters.indexOf(c.toInt) >= 0)
    if (weekBased.nonEmpty) {
      issues += s"uses week-based letter(s) ${quoteLetters(weekBased)} (all week-based patterns " +
        "are unsupported since Spark 3.0)"
      if (weekBased.contains('Y')) suggestions += "Y -> y (year-of-era)"
      if (weekBased.exists(c => c == 'W' || c == 'w')) {
        suggestions += "W/w (week-of-month/week-of-year) -> drop them or compute with weekofyear()"
      }
      if (weekBased.exists(c => c == 'u' || c == 'e' || c == 'c')) {
        suggestions += "u/e/c (day-of-week number) -> E (formatting only) or compute with dayofweek()"
      }
    }

    val illegal = runs.map(_._1).filter(c => illegalLetters.indexOf(c.toInt) >= 0)
    if (illegal.nonEmpty) {
      issues += s"uses ${quoteLetters(illegal)} which is not a supported Spark 3 pattern letter"
      suggestions += "A/B/n/N/p -> no equivalent; use the H/m/s/S fields instead"
    }

    if (parsing) {
      val parsingOnly = runs.map(_._1).filter(c => parsingOnlyLetters.indexOf(c.toInt) >= 0)
      if (parsingOnly.nonEmpty) {
        issues += s"uses ${quoteLetters(parsingOnly)} which can only be used for formatting " +
          "(date_format/from_unixtime/writer options), not parsing " +
          "(to_date/to_timestamp/unix_timestamp/reader options)"
        suggestions += "E/F/q/Q -> remove them from the parse pattern (quote literal text with '...')"
      }
    } else if (runs.exists(_._1 == 'F')) {
      issues += "uses 'F' whose meaning changed from day-of-week-in-month (SimpleDateFormat, " +
        "= ceil(dayOfMonth/7)) to java.time aligned-day-of-week-in-month ((dayOfMonth-1)%7+1)"
      suggestions += "F -> compute ceil(dayofmonth(c) / 7) yourself if the old value is wanted"
    }

    val tooLong = runs.collect {
      case (c, n) if n >= 5 && lengthLimitedLetters.indexOf(c.toInt) >= 0 => c.toString * n
      case (c, n) if n >= 7 && c == 'y'                                   => c.toString * n
    }
    if (tooLong.nonEmpty) {
      issues += s"uses ${tooLong.map(s => s"'$s'").mkString(", ")} (too many pattern letters) which " +
        "fails in Spark 3"
      suggestions += "use at most 4 letters for text fields (e.g. EEEE, MMMM) and at most 6 for y"
    }

    if (issues.isEmpty) {
      Patch.empty
    } else {
      Patch.lint(DateTimePatternWarning(lit, pattern, parsing, issues.toList, suggestions.toList))
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(fn, List(_, lit @ Lit.String(pattern))) if formattingFunctions.matches(fn) =>
        check(lit, pattern, parsing = false)
      case Term.Apply(fn, List(_, lit @ Lit.String(pattern))) if parsingFunctions.matches(fn) =>
        check(lit, pattern, parsing = true)
      case Term.Apply(fn, List(Lit.String(key), lit @ Lit.String(pattern)))
          if patternOptionKeys.contains(key) && parsingOptions.matches(fn) =>
        check(lit, pattern, parsing = true)
      case Term.Apply(fn, List(Lit.String(key), lit @ Lit.String(pattern)))
          if patternOptionKeys.contains(key) && formattingOptions.matches(fn) =>
        check(lit, pattern, parsing = false)
    }.asPatch
  }
}
