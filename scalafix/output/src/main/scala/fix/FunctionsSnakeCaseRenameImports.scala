package fix

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{approx_count_distinct, sum_distinct, degrees => deg}
import org.apache.spark.sql.functions.col

object FunctionsSnakeCaseRenameImports {
  def inSource(df: DataFrame): DataFrame = {
    // The aliased import keeps its alias; only the imported name changes.
    df.select(sum_distinct(col("x")), deg(df("y")), approx_count_distinct(df("z")))
  }
}
