# Scalafix rules for Spark Auto Upgrade

To use the scalafix rules, see the build tool specific docs https://github.com/holdenk/spark-upgrade/tree/main/docs/scala
and the end to end demo https://github.com/holdenk/spark-upgrade/tree/main/e2e_demo/scala

To develop rule:
```
sbt ~tests/test
# edit rules/src/main/scala/fix/SparkAutoUpgrade.scala
```

## Other rules to use

https://github.com/scala/scala-rewrites -

fix.scala213.Any2StringAdd
fix.scala213.Core
fix.scala213.ExplicitNonNullaryApply
fix.scala213.ExplicitNullaryEtaExpansion
fix.scala213.NullaryHashHash
fix.scala213.ScalaSeq
fix.scala213.Varargs
