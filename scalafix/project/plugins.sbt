addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")

addDependencyTreePlugin

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

// 1.11.0+ publishes to the Central Portal (central.sonatype.com). The 1.5.x line
// only knew the legacy OSSRH host, which Sonatype shut down on 2025-06-30; every
// release run since has died with "HTTP response code: 405" from
// oss.sonatype.org/content/repositories/snapshots. SNAPSHOT builds now go to
// https://central.sonatype.com/repository/maven-snapshots/ (the plugin picks the
// URL from the dynver version), tagged builds through the Portal's bundle upload.
// Requires sbt 1.11+ (project/build.properties) and Portal *user-token* secrets.
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.12.1")
