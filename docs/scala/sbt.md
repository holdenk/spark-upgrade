Hi Friend! It looks your trying to migrate a ASF Spark project!
Let's make it happen!
These instructions are written for sbt, there will be instructions for other builds as well.

I've tried to update your build file for you, but there might be some mistakes. What I've tried to do is:

``
scalafixDependencies in ThisBuild +=
  "com.holdenkarau" %% "spark-scalafix-rules" % "0.1.1-2.4.8"
``

Then add:

``
resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")
``

to project/plugins.sbt

And update your build file to add a "-3" to the artifact name so I can tell the difference between your Spark 3 & Spark 2 jars.

Thanks friend!

(Note: we could also try and do this with some REs on your build file too, but... it's a demo)
