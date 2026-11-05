package fix

import org.apache.spark.sql.{DataFrame, Dataset}

// A user class with its own registerTempTable: must not be rewritten.
class RegisterTempTableRewriteUserCatalog {
  def registerTempTable(name: String): Unit = println(name)
}

object RegisterTempTableRewrite {
  def inSource(
      df: DataFrame,
      ds: Dataset[String],
      catalog: RegisterTempTableRewriteUserCatalog
  ): Unit = {
    df.createOrReplaceTempView("people")
    ds createOrReplaceTempView "strings"
    df.select("a").createOrReplaceTempView("projected")
    catalog.registerTempTable("untouched")
  }
}
