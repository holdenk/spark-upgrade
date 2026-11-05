/*
rule=IsRunningLocallyWarn
 */
package fix

import org.apache.spark.TaskContext

object IsRunningLocallyWarn {
  def inSource(): Boolean = {
    val local = TaskContext.get().isRunningLocally() // assert: IsRunningLocallyWarn
    local
  }
}
