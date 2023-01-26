import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.holdenkarau"
ThisBuild / organizationName := "holdenkarau"
ThisBuild / name := "Iceberg WAP plugin"

Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
Test / parallelExecution := false
Test / fork := true
// TODO: Put in java option to load our agent
// For now this should fail.
Test / javaOptions += "-faaaarts"



lazy val root = (project in file("."))
  .settings(
    name := "Iceberg Spark Upgrade WAP Plugin",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += icebergSparkRuntime % Test,
    libraryDependencies += sparkTestingBase % Test,
    libraryDependencies += iceberg % Provided,
    libraryDependencies += logback % Provided,
    libraryDependencies += scalaLogging,
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
