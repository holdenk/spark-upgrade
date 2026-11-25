addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")

addDependencyTreePlugin

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

// 1.11+ publishes to the Central Portal; 1.5.x targeted the retired OSSRH host.
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.12.1")
