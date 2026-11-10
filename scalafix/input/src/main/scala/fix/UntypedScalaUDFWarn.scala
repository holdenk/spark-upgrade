/*
rule=UntypedScalaUDFWarn
 */
package fix

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.{IntegerType, StringType}

object UntypedScalaUDFWarn {
  def inSource(spark: SparkSession): Unit = {
    val plusOne = (x: Int) => x + 1
    val untypedBare = udf(plusOne, IntegerType) // assert: UntypedScalaUDFWarn
    val untypedQualified = functions.udf((s: String) => s.toUpperCase, StringType) // assert: UntypedScalaUDFWarn
    val untypedAliased = F.udf((x: Int) => x + 1, IntegerType) // assert: UntypedScalaUDFWarn
    // Typed overloads: not flagged.
    val typedBare = udf((x: Int) => x + 1)
    val typedAliased = F.udf(plusOne)
    // Java UDF API with an explicit return type: not flagged.
    spark.udf.register(
      "plusOneJava",
      new UDF1[Int, Int] {
        override def call(x: Int): Int = x + 1
      },
      IntegerType
    )
  }
}
