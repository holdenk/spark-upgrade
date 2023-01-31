package com.holdenkarau.spark.upgrade.wap.plugin

import com.typesafe.scalalogging.Logger

import org.apache.iceberg.events.{CreateSnapshotEvent, Listener}

object WAPIcebergListener extends Listener[CreateSnapshotEvent] {
  val logger = Logger(getClass.getName)

  // For testing
  private[holdenkarau] var lastLog = ""

  override def notify(event: CreateSnapshotEvent): Unit = {
    val msg = s"IcebergListener: Created snapshot ${event.snapshotId()} on table " +
      s"${event.tableName()} summary ${event.summary()} from operation " +
    s"${event.operation()}"
    lastLog = msg
    logger.info(msg)
  }
}
