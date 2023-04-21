/*
 rule=UnaryExpr
 */

package com.holdenkarau.spark.misc.utils

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._


case class TestUnaryExpression(child: Expression) extends UnaryExpression
    with NonSQLExpression {
  override def prettyName = "test"

  override def nullSafeEval(input: Any): Any = {
    null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    null
  }

  override def dataType: DataType = BinaryType
}
