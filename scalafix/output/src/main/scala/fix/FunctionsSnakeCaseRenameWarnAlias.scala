package fix

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{sumDistinct => sd}

object FunctionsSnakeCaseRenameWarnAlias {
  // An eta expansion through an import alias freezes the file as well: nothing is renamed.
  def inSource(df: DataFrame, names: Seq[String]): Seq[Column] = {
    val eta: String => Column = sd 
    names.map(eta) :+ sd("a") :+ sd(df("b"))
  }
}
