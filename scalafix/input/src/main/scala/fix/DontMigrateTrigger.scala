/*
 rule=MigrateTrigger
 */
import scala.concurrent.duration._
import org.apache.spark._
import org.apache.spark.sql.streaming._

object DontMigrateTrigger {
  def boop(): Unit = {
    val sc = new SparkContext()
  }
}
