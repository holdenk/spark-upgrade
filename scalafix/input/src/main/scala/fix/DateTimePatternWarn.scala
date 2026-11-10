/*
rule=DateTimePatternWarn
 */
package fix

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

object DateTimePatternWarn {
  def inSource(spark: SparkSession, df: DataFrame): Unit = {
    val c = df("ts")
    // Formatting functions.
    val weekBased = date_format(c, "YYYY-ww") // assert: DateTimePatternWarn
    val tooManyLetters = functions.date_format(c, "MMMMM dd") // assert: DateTimePatternWarn
    val weekOfMonth = date_format(c, "F") // assert: DateTimePatternWarn
    val illegalLetter = from_unixtime(c, "yyyy-MM-dd hh:mm A") // assert: DateTimePatternWarn
    // Parsing functions.
    val dayNameParsed = unix_timestamp(c, "yyyy EEEE") // assert: DateTimePatternWarn
    val dayNumberParsed = unix_timestamp(c, "yyyy-MM-dd u") // assert: DateTimePatternWarn
    // Fine in Spark 3: text day-of-week is allowed when formatting, quoted text is skipped.
    val dayNameFormatted = date_format(c, "EEEE")
    val plain = unix_timestamp(c, "yyyy-MM-dd HH:mm:ss")
    val quoted = date_format(c, "yyyy-MM-dd'T'HH")
    val quotedParse = unix_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    val escapedQuote = date_format(c, "yyyy-MM-dd''HH")
    // Not plain literals: skipped.
    val fmt = "YYYY"
    val fromVal = date_format(c, fmt)
    val interpolated = date_format(c, s"$fmt-MM")
    // Reader options parse, writer options format.
    val reader = spark.read
      .option("dateFormat", "dd/MM/YYYY") // assert: DateTimePatternWarn
      .option("timestampFormat", "yyyy-MM-dd EEE HH:mm") // assert: DateTimePatternWarn
      .option("multiline", "YYYY")
      .option("dateFormat", "yyyy-MM-dd")
    val streamReader = spark.readStream
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.nnn") // assert: DateTimePatternWarn
      .option("dateFormat", "yyyy-MM-dd")
    val writer = df.write
      .option("timestampFormat", "YYYY-MM-dd HH:mm:ss") // assert: DateTimePatternWarn
      .option("dateFormat", "yyyy-MM-dd EEE")
    val streamWriter = df.writeStream
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      .option("dateFormat", "yyyy-ww") // assert: DateTimePatternWarn
  }
}
