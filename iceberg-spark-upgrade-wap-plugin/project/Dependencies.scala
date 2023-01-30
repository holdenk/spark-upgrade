import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.11"
  lazy val iceberg = "org.apache.iceberg" % "iceberg-core" % "0.9.1"
  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.2.10"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
  lazy val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "3.2.2_1.3.3-SNAPSHOT"
  lazy val icebergSparkRuntime = "org.apache.iceberg" %% "iceberg-spark-runtime-3.2" % "1.1.0"
}
