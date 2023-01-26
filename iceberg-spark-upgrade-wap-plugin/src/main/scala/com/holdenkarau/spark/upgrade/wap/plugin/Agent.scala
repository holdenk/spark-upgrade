package com.holdenkarau.spark.upgrade.wap.plugin

import java.lang.instrument.Instrumentation;

import org.apache.iceberg.events.{CreateSnapshotEvent, Listeners}

class Agent {
  def premain(agentOps: String, inst: Instrumentation): Unit = {
    registerListener()
  }
  def agentmain(agentOps: String, inst: Instrumentation): Unit = {
    registerListener()
  }
  def registerListener(): Unit = {
    Listeners.register(WAPIcebergListener, classOf[CreateSnapshotEvent])
  }
}
