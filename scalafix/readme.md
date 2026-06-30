# Scalafix rules for Spark Auto Upgrade

To use the scalafix rules, see the build tool specific docs https://github.com/holdenk/spark-upgrade/tree/main/docs/scala
and the end to end demo https://github.com/holdenk/spark-upgrade/tree/main/e2e_demo/scala

## Target Spark version

The source and target Spark versions used when running the test suite are controlled by the `sparkVersion` and
`targetSparkVersion` system properties (see `build.sbt`). To exercise the rules against a Spark 4.x target, run for
example:

```bash
sbt -DsparkVersion=3.5.1 -DtargetSparkVersion=4.0.0 tests/test
```

### Spark 4.x rules

The following lint rules cover the most impactful Spark 4.0 behavior changes that affect Scala pipelines:

- `AnsiModeEnabledWarn` - ANSI SQL mode (`spark.sql.ansi.enabled`) is enabled by default since Spark 4.0, so casts and
  arithmetic that used to return `null` on error may now throw.
- `MesosRemovedWarn` - Apache Mesos support was removed in Spark 4.0; migrate to YARN, Kubernetes, or Standalone.

Many other Spark 4.x changes are configuration/SQL behavior changes rather than Scala API signature changes; for those,
prefer restoring the legacy behavior via the config map in [`conf_migrate`](../conf_migrate/migrate.py) and the in-line
SQL rules. Note that Spark 4.2 is still a preview release.

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

A **conservative**, opt-in **rewrite** that swaps an RDD pipeline to the typed
`Dataset` API — but only when the whole file is migratable *and* the result is
guaranteed to compile to the same thing and compute the same result. The safe
surface is deliberately small; anything outside it is logged, not rewritten. When
safe it converts the RDD *origins* so the chain resolves to `Dataset` methods:

  - `<sess>.sparkContext.parallelize(seq)` / `makeRDD(seq)` → `<sess>.createDataset(seq)`
  - `<sess>.sparkContext.textFile(path)` → `<sess>.read.textFile(path)`
  - `someDataset.rdd` → `someDataset` (drops the `.rdd`)
  - `intersection` → `intersect` (both `INTERSECT DISTINCT`; semantics match)

Only the **single-argument** origin forms convert: `parallelize(seq, n)` and
`textFile(path, minPartitions)` are blocked, because `Dataset` can't reproduce RDD
partition slicing without a shuffle that reorders rows / drops the hint. Binary
ops (`union`, `intersection`) additionally require their **argument** to trace to a
convertible origin (else it would still be an RDD and the call wouldn't compile).

It makes **no change** and logs each blocker when the file uses anything outside
the safe set, including:

  - key/pair functions, joins, `sortBy`, `++`, RDD-only accessors;
  - `subtract` (RDD removes *every* row whose value is in the other RDD — a
    left-anti join, which neither `except` nor `exceptAll` reproduces), `sample`
    (different sampler/seed), `isEmpty` (`Dataset.isEmpty` is parameterless),
    `toLocalIterator` (returns a `java.util.Iterator`), `checkpoint` (returns a new
    Dataset);
  - higher arities of `coalesce`/`distinct`/`mapPartitions` (including the
    explicit-type-argument form `mapPartitions[U](f, true)`);
  - any op used as an eta-expanded method value (`rdd.map _`);
  - any file that declares an `RDD[...]` type (a return type / ascription would be
    left dangling once the value becomes a `Dataset`).

The typed `Dataset` operations need an `Encoder`, so an `import <session>.implicits._`
must be in scope; if it is missing the rule logs that it's needed rather than
rewriting (auto-inserting a top-level import of a local session wouldn't compile).
`createDataset`/`read` are driven by the session in `<x>.sparkContext`, else the one
whose `implicits._` are imported; a file with **more than one** `implicits._` import
is blocked as ambiguous.

Inherent, documented limitations (still rewritten): `parallelize(seq)`/`textFile(path)`
produce a different *partition count* than the RDD (per-row results are identical,
but partition-count-sensitive side effects like `foreachPartition` file counts
differ), and `distinct`/`intersect` dedup on the encoded representation rather than a
custom `equals`.
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
