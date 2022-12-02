import org.apache.spark._

object BadAcc {
  def boop(): Unit = {
    val sc = new SparkContext()
    val num = 0
    val numAcc = /*sc.accumulator(num)*/ null
    val litAcc = /*sc.accumulator(0)*/ null
    val litLongAcc = sc.longAccumulator
    val namedAcc = /*sc.accumulator(0, "cheese")*/ null
    val litDoubleAcc = sc.doubleAccumulator
    val rdd = sc.parallelize(List(1,2,3))
  }
}
