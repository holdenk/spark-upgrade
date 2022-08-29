/*
rule = ExecutorPluginWarn
*/

import org.apache.spark.ExecutorPlugin // assert: ExecutorPluginWarn

class TestExecutorPlugin() extends ExecutorPlugin { // assert: ExecutorPluginWarn
  override def shutdown() = {
  }
}
