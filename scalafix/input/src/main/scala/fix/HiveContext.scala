/*
 rule=MigrateHiveContext
 */
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object BadHiveContextMagic {
  def hiveContextFunc(sc: SparkContext): HiveContext = {
    val hiveContext1 = new HiveContext(sc)
    import hiveContext1.implicits._
    hiveContext1
  }

  def makeSparkConf() = {
    val sparkConf = new SparkConf(true)
    sparkConf
  }

  def throwSomeCrap() = {
    throw new RuntimeException("mr farts!")
  }
}
