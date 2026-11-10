/*
rule=FunctionsSnakeCaseRename
 */
package fix

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.functions._

object FunctionsSnakeCaseRename {
  // A user-defined method with the same name as a deprecated Spark helper: must not change.
  def shiftLeft(x: Int, bits: Int): Int = x << bits

  def inSource(df: DataFrame): DataFrame = {
    val a = df("a")
    val renamed = df.select(
      approxCountDistinct(a),
      approxCountDistinct("a", 0.05),
      monotonicallyIncreasingId(),
      toDegrees(a),
      toRadians("a"),
      bitwiseNOT(a),
      F.shiftLeft(a, 2),
      functions.shiftRight(a, 2),
      shiftRightUnsigned(a, 1),
      callUDF("myUdf", a, df("b")),
      sumDistinct(a),
      sumDistinct("b"),
      F.sumDistinct("c")
    )
    val colName = "d"
    val blockArg = sumDistinct { colName }
    val qualifiedBlockArg = F.sumDistinct { colName }
    val namedArg = sumDistinct(columnName = colName)
    val qualifiedNamedArg = F.sumDistinct(columnName = colName)
    // Neither of these resolve to org.apache.spark.sql.functions: left alone.
    val userShift = shiftLeft(1, 2)
    val mathDegrees = math.toDegrees(1.0)
    renamed
  }
}
