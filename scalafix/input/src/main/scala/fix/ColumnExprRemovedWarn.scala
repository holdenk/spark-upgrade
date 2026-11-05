/*
rule=ColumnExprRemovedWarn
 */
package fix

import org.apache.spark.sql.{Column, DataFrame, TypedColumn}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.functions

// An unrelated class with an `expr` member: must not be flagged.
class ColumnExprRemovedWarnHolder(val expr: String)

// The removed constructor used from an extends clause.
class ColumnExprRemovedWarnMyColumn(e: Expression) extends Column(e) // assert: ColumnExprRemovedWarn

object ColumnExprRemovedWarn {
  def someExpr(): Expression = Literal(1)

  def inSource(
      df: DataFrame,
      holder: ColumnExprRemovedWarnHolder,
      exprs: Seq[Expression],
      name: String
  ): Unit = {
    val c = df("a")
    val catalystExpr: Expression = c.expr // assert: ColumnExprRemovedWarn
    val dataType = df("b").expr.dataType // assert: ColumnExprRemovedWarn
    val fromExpr = new Column(Literal(1)) // assert: ColumnExprRemovedWarn
    val typed = new TypedColumn[Int, Int](Literal(1), ExpressionEncoder[Int]()) // assert: ColumnExprRemovedWarn
    // Arguments whose static type is a type parameter: flagged because this file uses catalyst.
    val fromHead = new Column(exprs.head) // assert: ColumnExprRemovedWarn
    val fromIndex = new Column(exprs(0)) // assert: ColumnExprRemovedWarn
    val fromChild = new Column(someExpr().children.head) // assert: ColumnExprRemovedWarn
    val transformed = new Column(Literal(1).transform { case x => x }) // assert: ColumnExprRemovedWarn
    // Still valid in Spark 4.0: not flagged.
    val byName = new Column("a")
    val concatenated = new Column("prefix_" + name)
    val interpolated = new Column(s"prefix_$name")
    val conditional = new Column(if (name.isEmpty) "a" else "b")
    val matched = new Column(name match {
      case "" => "a"
      case _  => "b"
    })
    val sqlExpr = functions.expr("a + 1")
    val userExpr = holder.expr
  }
}
