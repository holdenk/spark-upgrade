/*
rule=RenamedSparkConfRewrite
 */
package fix

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RenamedSparkConfRewrite {
  def inSource(spark: SparkSession): Unit = {
    val conf = new SparkConf()
      .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      // Deprecated but not renamed here: the new key does not exist on Spark 3.x.
      .set("spark.shuffle.unsafe.file.output.buffer", "64k")
    val session = SparkSession.builder
      .config("spark.sql.legacy.avro.datetimeRebaseModeInRead", "CORRECTED")
      .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
    val settings = Map("spark.sql.legacy.avro.datetimeRebaseModeInWrite" -> "LEGACY")
    val tripleQuoted = spark.conf.get("""spark.sql.legacy.parquet.int96RebaseModeInWrite""")
    val unrelated = spark.conf.get("spark.sql.legacy.timeParserPolicy")
    // Fragments of a key (interpolation with arguments, concatenation) are not rewritten.
    val suffix = "X"
    spark.conf.set(s"spark.sql.legacy.parquet.int96RebaseModeInWrite$suffix", "LEGACY")
    spark.conf.set("spark.sql.legacy.parquet." + "int96RebaseModeInWrite", "LEGACY")
    // An interpolation without arguments is the whole key.
    spark.conf.set(s"spark.sql.legacy.avro.datetimeRebaseModeInRead", "CORRECTED")
  }
}
