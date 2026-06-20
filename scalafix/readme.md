# Scalafix rules for Spark Auto Upgrade

To use the scalafix rules, see the build tool specific docs https://github.com/holdenk/spark-upgrade/tree/main/docs/scala
and the end to end demo https://github.com/holdenk/spark-upgrade/tree/main/e2e_demo/scala

To migrate in-line SQL you will need to install sqlfluff + our extensions, which you can do with

```bash
git clone https://github.com/holdenk/spark-upgrade.git
cd spark-upgrade/sql; pip install .
```

## Optional rules

### RDDToDatasetMigrationCheck

A simplistic, opt-in check that looks at the RDD usage in a file and decides
whether it is "simple enough" to migrate to the typed `Dataset` API. It does
*not* rewrite your code; it only reports a lint message:

  - If every RDD operation used has a direct `Dataset`/`DataFrame` equivalent
    (`map`, `filter`, `flatMap`, `distinct`, `union`, `count`, ...), it reports
    that the pipeline is a good candidate for migration.
  - If any RDD operation has no simple equivalent (key/pair functions such as
    `reduceByKey`/`join`, `zipWithIndex`, custom `partitionBy`, manual
    `aggregate`, `saveAs*`, ...), it reports a warning at each such operation
    explaining why the pipeline can not be migrated automatically.

To keep it conservative, any RDD API the rule does not explicitly recognise is
treated as blocking a migration. It is not enabled in the default
`.scalafix.conf`; add it to your config (it ships in `.scalafix-warn.conf`) to
turn it on:

```
rules = [
  RDDToDatasetMigrationCheck
]
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
