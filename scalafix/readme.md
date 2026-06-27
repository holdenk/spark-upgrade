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

### RDDToDatasetMigration (rewrite)

A best-effort, opt-in **rewrite** that actually swaps an RDD pipeline to the
typed `Dataset` API — but only when the whole file is migratable. If every RDD
operation is a name-for-name `Dataset` swap (`map`, `filter`, `flatMap`,
`distinct`, `union`, `count`, `collect`, `reduce`, ...), it converts the RDD
*origins* and leaves the rest of the chain untouched:

  - `sc.parallelize(seq)` / `makeRDD(seq)` → `spark.createDataset(seq)`
    (`.repartition(n)` is appended when a partition count is given)
  - `sc.textFile(path)` → `spark.read.textFile(path)`
  - `someDataset.rdd` → `someDataset` (drops the `.rdd`)
  - `intersection` → `intersect`, `subtract` → `exceptAll` (renamed in place, only
    when both operands *trace* to a convertible origin so they are guaranteed
    Datasets; `exceptAll`, not `except`, because `RDD.subtract` keeps duplicates)

If the file uses anything that is not a clean swap (key/pair functions, joins,
`sortBy`, `++`, RDD-only accessors, an `intersection`/`subtract` whose operand
doesn't trace to a convertible origin, or an arity with no Dataset namesake such
as `coalesce(n, shuffle)` / `distinct(n)` / `mapPartitions(f, preserves)`), it
makes **no change** and logs each blocker instead — so it never half-migrates a
pipeline. (Ops whose Dataset namesake differs even at the base arity, like
`toLocalIterator` and `checkpoint`, are treated as blockers too.) The typed
`Dataset` operations need an `Encoder`, so an `import <session>.implicits._` must
be in scope; if it is missing the rule logs that it's needed rather than
rewriting (auto-inserting a top-level import of a local session wouldn't
compile). `createDataset`/`read` are driven by the session in `<x>.sparkContext`,
else the session whose `implicits._` are imported.
The result is meant to be recompiled, which validates the migration. See
[rdd-to-dataset-rewrite-design.md](./rdd-to-dataset-rewrite-design.md) for the
full design and the limits of what is rewritten. Enable it explicitly:

```
rules = [
  RDDToDatasetMigration
]
```

This is Scala-only. PySparkler's equivalent stays detection/log-only because
PySpark's `DataFrame` has no typed `map`/`flatMap`/`reduce` to swap to.

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
