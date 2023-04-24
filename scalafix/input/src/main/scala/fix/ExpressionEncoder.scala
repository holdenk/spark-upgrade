/*
 rule=ExpressionEncoder
 */
import org.apache.spark._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object BadEncoderEx {
  def boop() = {
    // Round trip with toRow and fromRow.
    val stringEncoder = ExpressionEncoder[String]
    val intEncoder = ExpressionEncoder[Int]
    val row = stringEncoder.toRow("hello world")
    val decoded = stringEncoder.fromRow(row)
    val intRow = intEncoder.toRow(1)
  }
}
