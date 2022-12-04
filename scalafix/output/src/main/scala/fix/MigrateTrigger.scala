import scala.concurrent.duration._
import org.apache.spark._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger._

object MigrateTrigger {
  def boop(): Unit = {
    val sc = new SparkContext()
    val trigger = ProcessingTime(1.second)
  }
}
