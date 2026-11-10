package fix

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum_distinct => sd}
import org.apache.spark.sql.functions.col

object FunctionsSnakeCaseRenameAlias {
  def inSource(df: DataFrame): DataFrame = {
    // The alias stays; the String overload still has to be wrapped in col(...).
    df.select(sd(col("x")), sd(df("y")), sd(col({ "z" })), sd(col("w")))
  }
}
