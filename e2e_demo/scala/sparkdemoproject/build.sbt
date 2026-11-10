// give the user a nice default project!

val sparkVersion = settingKey[String]("Spark version")

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "com.holdenkarau",
      scalaVersion := "2.12.13"
    )),
    name := "sparkDemoProject",
    version := "0.0.1",

    sparkVersion := "2.4.8",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,

    coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "2.4.8" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",

      "org.scalatest" %% "scalatest" % "3.2.2" % "test",
      "org.scalacheck" %% "scalacheck" % "1.15.2" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "2.4.8_1.3.0" % "test"
    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },

    // Everything resolves from Maven Central. The oss.sonatype.org (OSSRH) and
    // repo.typesafe.com resolvers that used to be listed here are both retired
    // hosts; sbt would only have waited on them for artifacts that were never
    // there. The demo never publishes, so the OSSRH publishTo block is gone too.
    pomIncludeRepository := { _ => false }
  )
