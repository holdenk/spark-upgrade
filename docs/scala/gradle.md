Hi Friend! It looks your trying to migrate a ASF Spark project!
Let's make it happen!
These instructions are written for gradle, there will be instructions for other builds as well.

I've tried to update your build file for you, but there might be some mistakes. What I've tried to do is:

Add
``
	scalafix group: "com.holdenkarau", name: 'spark-scalafix-rules-2.4.8_2.12', version: '0.1.13'
``
to your dependencies

And add:

``
	id "io.github.cosmicsilence.scalafix" version "0.1.14"
``

To your plugins.

If your including ScalaFix through "classpath" rather than "plugins" you will want add `apply plugin: 'io.github.cosmicsilence.scalafix'`.


And update your build file to add a "-3" to the artifact name so we can tell the difference between your Spark 3 & Spark 2 jars.

Thanks friend!

(Note: we could also try and do this with some REs on your build file too, but... it's a demo)

Add a .scalafix.conf file as patterned after the one in our scalafix directory.
