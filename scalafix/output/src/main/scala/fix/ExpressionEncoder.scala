import org.apache.spark._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object BadEncoderEx {
  def boop() = {
    // Round trip with toRow and fromRow.
    val stringEncoder = ExpressionEncoder[String]
    val intEncoder = ExpressionEncoder[Int]
    val row = stringEncoder.createSerializer()("hello world")
    val decoded = stringEncoder.createDeserializer()(row)
    val intRow = intEncoder.createSerializer()(1)
  }
}
