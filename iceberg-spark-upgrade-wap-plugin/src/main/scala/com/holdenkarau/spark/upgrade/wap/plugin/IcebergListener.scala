package com.holdenkarau.spark.upgrade.wap.plugin

import org.apache.iceberg.events.{CreateSnapshotEvent, Listener}

object WAPIcebergListener extends Listener[CreateSnapshotEvent] {
  // For testing
  private[holdenkarau] var lastLog = ""

  override def notify(event: CreateSnapshotEvent): Unit = {
    val msg = s"IcebergListener: Created snapshot ${event.snapshotId()} on table " +
      s"${event.tableName()} summary ${event.summary()} from operation " +
    s"${event.operation()}"
    lastLog = msg
    System.err.println(msg)
  }
}
