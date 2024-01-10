package fix

import scalafix.v1._
import scala.meta._
import scala.util.matching.Regex
import scala.collection.mutable.HashSet
import scala.reflect.ClassTag

case class Utils()(implicit val doc: SemanticDocument) {
  /**
   * Match an RDD. Just the symbol matcher alone misses some cases so we also
   * look at type signatures etc.
   */
  class MagicMatcher(matchers: List[SymbolMatcher]) {
    def unapply(param: Term) = {
      matchers.flatMap(unapplyMatcher(_, param)).headOption
    }

    def unapplyMatcher(matcher: SymbolMatcher, param: Term) = {
      param match {
        case matcher(e) => Some(e)
        case _ =>
          param.symbol.info match {
            case None =>
              None
            case Some(symbolInfo) =>
              symbolInfo.signature match {
                case ValueSignature(tpe) =>
                  tpe match {
                    case TypeRef(_, symbol, _) =>
                      symbol match {
                        case matcher(e) => Some(param)
                        case _ => None
                      }
                    case _ =>
                      None
                  }
                case _ => None
              }
          }
      }
    }

  }

  /**
   * Strings, ints, doubles, etc. can all be literals or regular symbols
   */
  class MagicMatcherLit[T <: meta.Lit: ClassTag](matchers: List[SymbolMatcher])
      extends MagicMatcher(matchers) {
    override def unapply(param: Term) = {
      param match {
        case e: T => Some(e)
        case _ => super.unapply(param)
      }
    }
  }


  object intMatcher extends MagicMatcherLit[Lit.Int](
    List(SymbolMatcher.normalized("scala.Int")))
  object longMatcher extends MagicMatcherLit[Lit.Long](
    List(SymbolMatcher.normalized("scala.Long")))
  object doubleMatcher extends MagicMatcherLit[Lit.Double](
    List(SymbolMatcher.normalized("scala.Double")))

  object rddMatcher extends MagicMatcher(
    List(SymbolMatcher.normalized("org.apache.spark.rdd.RDD#")))

  lazy val imports = HashSet(doc.tree.collect {
    case Importer(term, importees) =>
        importees.map {
          importee => (term.toString(), importee.toString())
        }
    }.flatten:_*)

  private val importSplitRegex = "(.*?)\\.([a-zA-Z0-9_]+)".r

  /**
   * Add an import if the import it self is not present &
   * there is no corresponding import for this. Note this may make
   * mistakes with rename imports & local imports.
   */
  def addImportIfNotPresent(importElem: Importer): Patch = {
    val importName = importElem.toString()
    importName match {
      case importSplitRegex(importTermName, importee) =>
        if (imports contains ((importTermName, importee))) {
          throw new IllegalArgumentException("Import not supported: " + importElem.toString())
        } else if (imports contains ((importTermName, "_"))) {
          Patch.empty
        } else {
          importElem match {
            case Importer(term, importees) =>
              imports ++= importees.map {
                importee => (term.toString(), importee.toString())
              }
          }
          Patch.addGlobalImport(importElem)
        }
    }
  }

  def importPresent(importName: String): Boolean = {
    true
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(getClass)

private def logError(message: String, exception: Throwable): Unit = {
  logger.error(message, exception)
}

private def logInfo(message: String): Unit = {
  logger.info(message)
}  }
}
