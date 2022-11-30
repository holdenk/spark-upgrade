/*
rule=MetadataWarnQQ
 */
package fix

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.Metadata // assert: MetadataWarnQQ

object MetadataWarnQQ{
  def inSource(sparkSession: SparkSession, df: DataFrame): Unit = {
    val ndf = df.select(
      col("id"),
      col("v").as(
        "newV",
        Metadata.fromJson( // assert: MetadataWarnQQ
          """{"desc": "replace old V"}"""
        )
      )
    )
  }
}
