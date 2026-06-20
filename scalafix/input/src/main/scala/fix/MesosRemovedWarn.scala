/*
rule=MesosRemovedWarn
 */
package fix

import org.apache.spark.SparkConf

object MesosRemovedWarn {
  def inSource(): Unit = {
    val conf = new SparkConf()
      .setMaster("mesos://host:5050") // assert: MesosRemovedWarn
      .setAppName("example")
  }
}
