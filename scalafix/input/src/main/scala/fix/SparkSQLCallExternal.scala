/*
 rule=SparkSQLCallExternal
 */
import org.apache.spark._
import org.apache.spark.sql._

object OldQuery {
  def doQuery(s: SparkSession) {
    // We should be able to rewrite this one
    s.sql("""select
      cast(a as int),
      cast(b as int)
    from fart_tbl""")
    // We can't auto rewrite this :( easily.
    val q = "SELECT * FROM FARTS LIMIT 1"
    s.sql(q)
    // we should not change this
    fart("magic farts")
  }

  def fart(str: String) = {
    println(s"Fart ${str}")
  }
}
