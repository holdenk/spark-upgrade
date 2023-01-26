package com.holdenkarau.spark.upgrade.wap.plugin

import com.typesafe.scalalogging.Logger

import org.apache.iceberg.events.{CreateSnapshotEvent, Listener}

object WAPIcebergListener extends Listener[CreateSnapshotEvent] {
  val logger = Logger(getClass.getName)

  // For testing
  private[holdenkarau] var lastLog = ""

  override def notify(event: CreateSnapshotEvent): Unit = {
    val msg = s"Created snapshot {createSnapshotEvent.snapshotId()} on table " +
      s"{createSnapshotEvent.tableName()} summary {createSnapshotEvent.summary()} from operation " +
    s"createSnapshotEvent.operation()"
    lastLog = msg
    logger.info(s"Created snapshot {createSnapshotEvent.snapshotId()} on table " +
      s"{createSnapshotEvent.tableName()} summary {createSnapshotEvent.summary()} from operation " +
      s"createSnapshotEvent.operation()")
  }
}
