/*
rule=FunctionsSnakeCaseRename
 */
package fix

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sumDistinct => sd}

object FunctionsSnakeCaseRenameAlias {
  def inSource(df: DataFrame): DataFrame = {
    // The alias stays; the String overload still has to be wrapped in col(...).
    df.select(sd("x"), sd(df("y")), sd { "z" }, sd(columnName = "w"))
  }
}
