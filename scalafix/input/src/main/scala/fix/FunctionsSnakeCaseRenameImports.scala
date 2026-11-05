/*
rule=FunctionsSnakeCaseRename
 */
package fix

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{approxCountDistinct, sumDistinct, toDegrees => deg}

object FunctionsSnakeCaseRenameImports {
  def inSource(df: DataFrame): DataFrame = {
    // The aliased import keeps its alias; only the imported name changes.
    df.select(sumDistinct("x"), deg(df("y")), approxCountDistinct(df("z")))
  }
}
