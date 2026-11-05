package fix

import scalafix.v1._
import scala.meta._
import scala.util.{Failure, Success, Try}

case class UntypedScalaUDFWarning(tn: scala.meta.Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    """functions.udf(f, returnType) is the untyped Scala UDF. Since Spark 3.0 it throws at UDF
      |creation unless spark.sql.legacy.allowUntypedScalaUDF=true, and when a primitive-typed
      |argument receives null it returns the Java default (0/false) instead of null.
      |Preferred fix: drop the DataType argument so the return type is inferred (udf(f)), making
      |sure f has concrete parameter and return types -- note the inferred type may differ from
      |the DataType you declared, so check the schema. Temporary bridge: set
      |spark.sql.legacy.allowUntypedScalaUDF=true (still honoured in Spark 4.0).""".stripMargin
}

/**
 * Flags calls to the untyped `functions.udf(f: AnyRef, dataType: DataType)` overload. The
 * resolved overload's signature is inspected so the typed `udf(f)` overloads (type params, one
 * value param) and the Java `udf(f: UDFn, returnType: DataType)` overloads (first param is UDFn)
 * are never flagged.
 */
class UntypedScalaUDFWarn extends SemanticRule("UntypedScalaUDFWarn") {
  override val description =
    "Warn on functions.udf(f: AnyRef, dataType: DataType), disallowed by default since Spark 3.0."

  val udfMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.udf")

  private val anyRefTypes = Set("scala/AnyRef#", "java/lang/Object#")
  private val dataType = "org/apache/spark/sql/types/DataType#"

  private def typeSymbol(param: SymbolInformation): Option[String] = {
    param.signature match {
      case ValueSignature(TypeRef(_, sym, _)) => Some(sym.value)
      case _                                  => None
    }
  }

  private def signatureIsUntyped(sym: Symbol)(implicit doc: SemanticDocument): Boolean = {
    sym.info.map(_.signature) match {
      case Some(MethodSignature(Nil, List(List(first, second)), _)) =>
        typeSymbol(first).exists(anyRefTypes.contains) && typeSymbol(second).contains(dataType)
      case _ => false
    }
  }

  private val javaUdfPrefix = "org/apache/spark/sql/api/java/UDF"

  private def isJavaUDFType(tpe: Type)(implicit doc: SemanticDocument): Boolean =
    tpe.symbol.value.startsWith(javaUdfPrefix)

  /** True when the argument is visibly an org.apache.spark.sql.api.java.UDFn instance. */
  private def looksLikeJavaUDF(term: Term)(implicit doc: SemanticDocument): Boolean = {
    term match {
      case anon: Term.NewAnonymous => anon.templ.inits.exists(init => isJavaUDFType(init.tpe))
      case Term.New(init)          => isJavaUDFType(init.tpe)
      case Term.Ascribe(_, tpe)    => isJavaUDFType(tpe)
      case other                   =>
        Try(other.symbol.info.map(_.signature)).toOption.flatten match {
          case Some(ValueSignature(TypeRef(_, sym, _))) => sym.value.startsWith(javaUdfPrefix)
          case _                                        => false
        }
    }
  }

  private def isUntypedOverload(
      fn: Term,
      firstArg: Term
  )(implicit doc: SemanticDocument): Boolean = {
    Try(signatureIsUntyped(fn.symbol)) match {
      case Success(untyped) => untyped
      case Failure(_)       =>
        // The classpath symbol table could not be read (old scalameta on a modern JDK, as in the
        // Scala 2.11 build on JDK 11). The only two-argument `udf` overloads are the untyped one
        // and the Java `udf(UDFn, DataType)` ones, so flag unless the first argument is a Java UDF.
        !looksLikeJavaUDF(firstArg)
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    if (doc.input.text.contains("udf")) {
      doc.tree.collect {
        case call @ Term.Apply(fn, List(first, _))
            if udfMatcher.matches(fn) && isUntypedOverload(fn, first) =>
          Patch.lint(UntypedScalaUDFWarning(call))
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
