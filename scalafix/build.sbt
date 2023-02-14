val sparkVersion = settingKey[String]("Spark version")
val srcSparkVersion = settingKey[String]("Source Spark version")
val targetSparkVersion = settingKey[String]("Target Spark version")
val sparkUpgradeVersion = settingKey[String]("Spark upgrade version")


lazy val V = _root_.scalafix.sbt.BuildInfo
inThisBuild(
  List(
    organization := "com.holdenkarau",
    homepage := Some(url("https://github.com/holdenk/spark-auto-upgrade")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    srcSparkVersion := System.getProperty("sparkVersion", "2.4.8"),
    targetSparkVersion := System.getProperty("targetSparkVersion", "3.3.0"),
    sparkVersion := srcSparkVersion.value,
    sparkUpgradeVersion := "0.1.6", // latest version tagged.
    versionScheme := Some("early-semver"),
    publishMavenStyle := true,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    useGpg := true,
    developers := List(
      Developer(
        "holdenk",
        "Holden Karau",
        "holden@pigscanfly.ca",
        url("https://github.com/holdenk/spark-auto-upgrade")
      )
    ),
    scalaVersion := V.scala212,
    crossScalaVersions := {
      if (sparkVersion.value > "3.1.0") {
        List(V.scala212, V.scala213)
      } else {
        List(V.scala211, V.scala212)
      }
    },
    addCompilerPlugin(scalafixSemanticdb),
    scalacOptions ++= List(
      "-Yrangepos",
      "-P:semanticdb:synthetics:on"
    ),
    scmInfo := Some(ScmInfo(
      url("https://github.com/holdenk/spark-testing-base.git"),
      "scm:git@github.com:holdenk/spark-testing-base.git"
    )),
    skip in publish := false
  )
)

skip in publish := true


lazy val rules = project.settings(
  moduleName := s"spark-scalafix-rules-${sparkVersion.value}",
  libraryDependencies += "ch.epfl.scala" %% "scalafix-core" % V.scalafixVersion,
)

lazy val input = project.settings(
  skip in publish := true,
  sparkVersion := srcSparkVersion.value,
  libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.0",
    "org.apache.spark" %% "spark-core"        % sparkVersion.value,
    "org.apache.spark" %% "spark-sql"         % sparkVersion.value,
    "org.apache.spark" %% "spark-hive"        % sparkVersion.value,
    "org.scalatest" %% "scalatest" % "3.0.0")
)

lazy val output = project.settings(
  skip in publish := true,
  sparkVersion := targetSparkVersion.value,
  scalaVersion := V.scala212,
  crossScalaVersions := {
    if (sparkVersion.value > "3.1.0") {
      List(V.scala212, V.scala213)
    } else {
      List(V.scala211, V.scala212)
    }
  },
  libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.0",
    "org.apache.spark" %% "spark-core"        % sparkVersion.value,
    "org.apache.spark" %% "spark-sql"         % sparkVersion.value,
    "org.apache.spark" %% "spark-hive"        % sparkVersion.value,
    "org.scalatest" %% "scalatest" % "3.2.14")
)

lazy val tests = project
  .settings(
    skip in publish := true,
    libraryDependencies += "ch.epfl.scala" % "scalafix-testkit" % V.scalafixVersion % Test cross CrossVersion.full,
    compile.in(Compile) :=
      compile.in(Compile).dependsOn(compile.in(input, Compile), compile.in(output, Compile)).value,
    scalafixTestkitOutputSourceDirectories :=
      sourceDirectories.in(output, Compile).value,
    scalafixTestkitInputSourceDirectories :=
      sourceDirectories.in(input, Compile).value,
    scalafixTestkitInputClasspath :=
      fullClasspath.in(input, Compile).value,
  )
  .dependsOn(rules)
  .enablePlugins(ScalafixTestkitPlugin)

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
