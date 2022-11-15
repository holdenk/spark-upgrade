resolvers += Resolver.sonatypeRepo("releases")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")

addDependencyTreePlugin

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.11")
