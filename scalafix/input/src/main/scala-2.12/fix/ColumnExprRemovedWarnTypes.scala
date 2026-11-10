/*
rule=ColumnExprRemovedWarn
 */
package fix

import org.apache.spark.sql.{Column, DataFrame}

// Negatives that need the argument's type resolved through the classpath symbol table, which
// only works on the Scala 2.12+ build (hence this file lives in scala-2.12). This file does not
// import catalyst, so an unresolvable type is not flagged either.
object ColumnExprRemovedWarnTypes {
  def someString(): String = "s"

  def inSource(df: DataFrame, name: String): Unit = {
    val fromName = new Column(name)
    val fromMethod = new Column(someString())
    val fromCall = new Column(name.trim)
    val fromConcat = new Column(name + "_suffix")
    val fromOption = new Column(Some(name).get)
    val fromExpr = new Column(df("a").expr) // assert: ColumnExprRemovedWarn
  }
}
