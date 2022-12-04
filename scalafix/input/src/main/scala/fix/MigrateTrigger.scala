/*
 rule=MigrateTrigger
 */
import scala.concurrent.duration._
import org.apache.spark._
import org.apache.spark.sql.streaming._

object MigrateTrigger {
  def boop(): Unit = {
    val sc = new SparkContext()
    val trigger = ProcessingTime(1.second)
  }
}
