package fix

import java.io._


import scalafix.v1._
import scala.meta._
import sys.process._

class SparkSQLCallExternal extends SemanticRule("SparkSQLCallExternal") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val sparkSQLFunMatch = SymbolMatcher.normalized("org.apache.spark.sql.SparkSession.sql")
    val utils = new Utils()

    def matchOnTree(e: Tree): Patch = {
      e match {
        // non-named accumulator
        case ns @ Term.Apply(j @ sparkSQLFunMatch(f), params) =>
          // Find the spark context for rewriting
          params match {
            case List(param) =>
              param match {
                case s @ Lit.String(_) =>
                  // Write out the SQL to a file
                  val f = File.createTempFile("magic", ".sql")
                  f.deleteOnExit()
                  val bw = new BufferedWriter(new FileWriter(f))
                  bw.write(s.value.toString)
                  bw.close()
                  // Run SQL fluff
                  val strToRun = s"sqlfluff  fix --dialect sparksql -f ${f.toPath}"
                  println(s"Running ${strToRun}")
                  val ret = strToRun.!
                  println(ret)
                  val newSQL = scala.io.Source.fromFile(f).mkString
                  // We don't care about whitespace only changes.
                  if (newSQL.filterNot(_.isWhitespace) != s) {
                    Patch.replaceTree(param, "\"\"\"" + newSQL + "\"\"\"")
                  } else {
                    Patch.empty
                  }
                case _ =>
                  // TODO: Do we want to warn here about non migrated dynamically generated SQL
                  // or no?
                  Patch.empty
              }
            case _ =>
              Patch.empty
          }
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}
