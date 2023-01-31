import Dependencies._

resolvers += Resolver.mavenLocal
resolvers += Resolver.sonatypeRepo("public" )
resolvers += Resolver.typesafeRepo("releases")
resolvers += Resolver.sbtPluginRepo("releases")


ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.holdenkarau"
ThisBuild / organizationName := "holdenkarau"
ThisBuild / name := "Iceberg WAP plugin"
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
Test / parallelExecution := false
Test / fork := true
Test / javaOptions += "-javaagent:./target/scala-2.12/iceberg-spark-upgrade-wap-plugin_2.12-0.1.0-SNAPSHOT.jar"
Test / compile := ((Test / compile) dependsOn( Compile / Keys.`package` )).value


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

// Since sbt generates a MANIFEST.MF file rather than storing one in resources and dealing the conflict
// just add our properties to the one sbt generates for us.
Compile / packageBin / packageOptions ++= List(
  Package.ManifestAttributes("Premain-Class" -> "com.holdenkarau.spark.upgrade.wap.plugin.Agent"),
  Package.ManifestAttributes("Agent-Class" -> "com.holdenkarau.spark.upgrade.wap.plugin.Agent"),
  Package.ManifestAttributes("Can-Redefine-Classes" -> "com.holdenkarau.spark.upgrade.wap.plugin.Agent"))
