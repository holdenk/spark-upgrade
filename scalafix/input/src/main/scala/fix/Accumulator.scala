/*
 rule=AccumulatorUpgrade
 */
import org.apache.spark._

object BadAcc {
  def boop(): Unit = {
    val sc = new SparkContext()
    val num = 0
    val numAcc = sc.accumulator(num)// assert: AccumulatorUpgrade
    val litAcc = sc.accumulator(0)// assert: AccumulatorUpgrade
    val litLongAcc = sc.accumulator(0L)
    val namedAcc = sc.accumulator(0, "cheese")// assert: AccumulatorUpgrade
    val litDoubleAcc = sc.accumulator(0.0)
    val rdd = sc.parallelize(List(1,2,3))
  }
}
