/*
rule = UnionRewrite
UnionRewrite.deprecatedMethod {
  "unionAll" = "union"
}
 */
package fix
import org.apache.spark.sql.{DataFrame, Dataset}

object UnionRewrite {
  def inSource(
    df1: DataFrame,
    df2: DataFrame,
    df3: DataFrame,
    ds1: Dataset[String],
    ds2: Dataset[String]
  ): Unit = {
    val res1 = df1.unionAll(df2)
    val res2 = df1.unionAll(df2).unionAll(df3)
    val res3 = Seq(df1, df2, df3).reduce(_ unionAll _)
    val res4 = ds1.unionAll(ds2)
    val res5 = Seq(ds1, ds2).reduce(_ unionAll _)
  }
}
