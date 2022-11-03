package fix

import org.apache.spark.sql.{DataFrame, Dataset}

object SparkAutoUpgrade {
  // Add code that needs fixing here.
  def depricatedUnionAll(
                          df1: DataFrame,
                          df2: DataFrame,
                          df3: DataFrame,
                          ds1: Dataset[String],
                          ds2: Dataset[String]
                        ): Unit = {
    val res1 = df1.union(df2)
    val res2 = df1.union(df2).union(df3)
    val res3 = Seq(df1, df2, df3).reduce(_ union _)
    val res4 = ds1.union(ds2)
    val res5 = Seq(ds1, ds2).reduce(_ union _)
  }
}
