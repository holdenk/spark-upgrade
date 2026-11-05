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
      approx_count_distinct(a),
      approx_count_distinct("a", 0.05),
      monotonically_increasing_id(),
      degrees(a),
      radians("a"),
      bitwise_not(a),
      F.shiftleft(a, 2),
      functions.shiftright(a, 2),
      shiftrightunsigned(a, 1),
      call_udf("myUdf", a, df("b")),
      sum_distinct(a),
      sum_distinct(col("b")),
      F.sum_distinct(F.col("c"))
    )
    val colName = "d"
    val blockArg = sum_distinct(col({ colName }))
    val qualifiedBlockArg = F.sum_distinct(F.col({ colName }))
    val namedArg = sum_distinct(col(colName))
    val qualifiedNamedArg = F.sum_distinct(F.col(colName))
    // Neither of these resolve to org.apache.spark.sql.functions: left alone.
    val userShift = shiftLeft(1, 2)
    val mathDegrees = math.toDegrees(1.0)
    renamed
  }
}
