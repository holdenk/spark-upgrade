/*
rule=UntypedScalaUDFWarn
 */
package fix

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.IntegerType

// functions.udf(f: UDF1, returnType: DataType) only exists from Spark 2.3, so this fixture is
// kept out of the Scala 2.11 (Spark 2.1.1) build.
object UntypedScalaUDFWarnJavaUDF {
  val javaUdf = udf(
    new UDF1[Int, Int] {
      override def call(x: Int): Int = x + 1
    },
    IntegerType
  )
  val untyped = udf((x: Int) => x + 1, IntegerType) // assert: UntypedScalaUDFWarn
}
