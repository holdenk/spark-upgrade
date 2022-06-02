import org.apache.spark._

object BadAcc {
  def boop(): Unit = {
    val sc = new SparkContext()
    val num = 0
    val numAcc = sc.accumulator(num)
    val litAcc = sc.accumulator(0)
    val litLongAcc = sc.longAccumulator
    val namedAcc = sc.accumulator(0, "cheese")
    val litDoubleAcc = sc.doubleAccumulator
    val rdd = sc.parallelize(List(1,2,3))
    rdd.foreach(x => numAcc += x)
  }
}
