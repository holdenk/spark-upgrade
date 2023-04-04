/*
rule=MultiLineDatasetReadWarn
 */
package fix
import org.apache.spark.sql.{SparkSession, Dataset}

class MultiLineDatasetReadWarn {
  def inSource(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val df = (sparkSession   // assert: MultiLineDatasetReadWarn
      .read
      .format("csv")
      .option("multiline", true)
    )

    val okDf = (sparkSession
      .read
      .format("csv")
    )

    val ds7 = Seq("test 1", "test 2", "test 3").toDF().groupBy("value").count()
  }

}
