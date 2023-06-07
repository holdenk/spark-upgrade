# Scalafix rules for Spark Auto Upgrade

To use the scalafix rules, see the build tool specific docs https://github.com/holdenk/spark-upgrade/tree/main/docs/scala
and the end to end demo https://github.com/holdenk/spark-upgrade/tree/main/e2e_demo/scala

To migrate in-line SQL you will need to install sqlfluff + our extensions, which you can do with

```bash
git clone https://github.com/holdenk/spark-upgrade.git
cd spark-upgrade/sql; pip install .
```

## Other (non-Spark Specific) rules you may wish to use

https://github.com/scala/scala-rewrites -

fix.scala213.Any2StringAdd
fix.scala213.Core
fix.scala213.ExplicitNonNullaryApply
fix.scala213.ExplicitNullaryEtaExpansion
fix.scala213.NullaryHashHash
fix.scala213.ScalaSeq
fix.scala213.Varargs


## Demo
You can also watch the end to end demo at https://www.youtube.com/watch?v=bqpb84n9Dpk :)

## Extending

To develop rule:
```
sbt ~tests/test
# edit rules/src/main/scala/fix/SparkAutoUpgrade.scala
```
