/*
rule=FunctionsSnakeCaseRename
 */
package fix

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.sumDistinct

object FunctionsSnakeCaseRenameWarn {
  // sumDistinct(String) uses that cannot be wrapped in col(...) are reported, not rewritten, and
  // the whole file then keeps sumDistinct (import and other calls included) so it still compiles.
  def inSource(names: Seq[String]): Seq[Column] = {
    val placeholder = names.map(sumDistinct(_)) // assert: FunctionsSnakeCaseRename
    val eta: String => Column = sumDistinct // assert: FunctionsSnakeCaseRename
    val frozen = sumDistinct("a")
    placeholder ++ names.map(eta) :+ frozen
  }
}
