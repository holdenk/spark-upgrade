/*
 rule=AllEquivalentExprs
 */
import org.apache.spark.sql.catalyst.expressions.EquivalentExpressions

object EETest {
  def boop(e: EquivalentExpressions) = {
    e.getAllEquivalentExprs
  }
}
