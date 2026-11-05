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
    (an explicit type argument is preserved: `parallelize[T](seq)` → `createDataset[T](seq)`,
    which keeps the empty-seq idiom compiling)
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
must be **lexically in scope at each rewritten site that needs one** (`createDataset`
origins and `map`/`flatMap`/`mapPartitions`) — an import inside a *different* method
doesn't count, and a file-wide check would wrongly rewrite such files into code that
doesn't compile. If it's missing at a site the rule logs that it's needed rather than
rewriting (auto-inserting a top-level import of a local session wouldn't compile).
`createDataset`/`read` are driven by the session in `<x>.sparkContext`, else the one
whose `implicits._` are imported; a file importing **two different sessions'**
`implicits._` is blocked as ambiguous (the same import repeated across methods — the
normal per-scope pattern — is fine).

Inherent, documented limitations (still rewritten): `parallelize(seq)`/`textFile(path)`
produce a different *partition count* than the RDD (per-row results are identical,
but partition-count-sensitive side effects like `foreachPartition` file counts
differ); `distinct`/`intersect` dedup on the encoded representation rather than a
custom `equals`; and a type *alias* of `RDD` (`type MyRDD = RDD[Int]`) used as an
annotation is not detected by the dangling-type guard.
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

## Deprecated API and configuration rules

These rules cover deprecated or removed APIs and configuration keys that commonly trip
2.x/3.x pipelines moving to Spark 3.x/4.x. Rewrites live in `.scalafix.conf`, lints in
`.scalafix-warn.conf`.

### FunctionsSnakeCaseRename (rewrite)

Renames the deprecated camelCase helpers in `org.apache.spark.sql.functions` to their
snake_case replacements: `approxCountDistinct` -> `approx_count_distinct`,
`monotonicallyIncreasingId` -> `monotonically_increasing_id`, `toDegrees` -> `degrees`,
`toRadians` -> `radians`, `bitwiseNOT` -> `bitwise_not`, `shiftLeft` -> `shiftleft`,
`shiftRight` -> `shiftright`, `shiftRightUnsigned` -> `shiftrightunsigned`,
`callUDF` -> `call_udf` and `sumDistinct` -> `sum_distinct`. Only the name token is
replaced, for bare calls (`import functions._`), qualified calls (`functions.x` / `F.x`) and
the importee in `import org.apache.spark.sql.functions.{toDegrees}` (an aliased import keeps
its alias). Matching is by resolved symbol, so a user-defined `def shiftLeft` or
`scala.math.toDegrees` is never touched. `sumDistinct(columnName: String)` has no
`sum_distinct(String)` counterpart, so it becomes `sum_distinct(col(columnName))` (adding
`import org.apache.spark.sql.functions.col` when the call is unqualified); this also applies through
an import alias (`import functions.{sumDistinct => sd}; sd("x")` becomes `sd(col("x"))`), to a block
argument (`sumDistinct { name }` -> `sum_distinct(col({ name }))`) and to a named argument
(`sumDistinct(columnName = name)` -> `sum_distinct(col(name))`). A use that cannot be wrapped
safely (an eta expansion, also through an alias, or a placeholder such as `sumDistinct(_)`) gets
a lint instead, and every `sumDistinct` in that file (the import included) is then left as is so
the file keeps compiling.

Caveat: `sum_distinct`, `bitwise_not`, `shiftleft`/`shiftright`/`shiftrightunsigned` and
`call_udf` only exist from Spark 3.2.0, so this rule needs a target Spark >= 3.2 (the default
target here is 3.3.0). The other renames exist since Spark 2.x. None of the old names has
been removed as of 4.0, so this is deprecation cleanup rather than a hard break.

### RegisterTempTableRewrite (rewrite)

Rewrites `Dataset.registerTempTable(name)` (deprecated since 2.0.0) to
`createOrReplaceTempView(name)`, for the dotted and infix forms. It matches the resolved
`org.apache.spark.sql.Dataset#registerTempTable` symbol, so a user class with its own
`registerTempTable` method is left alone. `registerTempTable` has been a one-line forwarder
to `createOrReplaceTempView` in every Spark version from 2.0 to 4.0, so the rewrite is
semantics preserving.

### UntypedScalaUDFWarn (lint)

Flags calls to the untyped `functions.udf(f: AnyRef, dataType: DataType)` overload, which
throws by default since Spark 3.0 (unless `spark.sql.legacy.allowUntypedScalaUDF=true`) and
changed null handling for primitive arguments. The resolved overload's signature is inspected,
so the typed `udf((x: Int) => ...)` overloads and the Java `udf(new UDF1[..]{..}, dataType)`
overloads are not flagged. Warn only: dropping the `DataType` argument can change the output
schema, so the fix is left to you. Caveat: when the rules jar is built for Scala 2.11 and run
on JDK 11+, scalameta cannot read the classpath symbol table, so the rule falls back to the shape
of the call (two arguments, first one not visibly a Java `UDFn`); `ColumnExprRemovedWarn` has the
same fallback (a `Column` constructor argument whose type cannot be resolved is flagged unless it
is a string literal, and only in files that plausibly deal with catalyst expressions).

### RenamedSparkConfRewrite (rewrite)

Rewrites string literals equal to the six `spark.sql.legacy.{parquet,avro}.*RebaseModeIn{Read,Write}`
keys to the same key without `.legacy` (quoting style preserved). Spark 4.0 removed five of the
six legacy keys and throws when one of them is set to anything but `CORRECTED`; the sixth,
`spark.sql.legacy.parquet.datetimeRebaseModeInRead`, is still accepted as an alternative and is
renamed for consistency. The new keys exist from Spark 3.0.0 (datetime) / 3.1.0 (int96), so the
rewrite is safe to apply before upgrading. A literal that is only a fragment of a key (a piece
of an interpolation with arguments such as `s"spark.sql.legacy.parquet.$name"`, or an operand of
`+`) is deliberately left alone; an interpolation without arguments is treated like a plain
literal. `spark.shuffle.unsafe.file.output.buffer` is deliberately not rewritten (its
replacement does not exist on Spark 3.x); `RemovedSparkConfWarn` warns about it instead.

### RemovedSparkConfWarn (lint)

Warns on string literals naming removed or renamed Spark configuration keys: the
`spark.blacklist.*` / `spark.scheduler.blacklist.*` / `spark.yarn.blacklist.*` family (renamed
to `spark.excludeOnFailure.*` in 3.1 and silently ignored since 4.1),
`spark.scheduler.executorTaskBlacklistTime`, `spark.shuffle.unsafe.file.output.buffer`
(deprecated in 4.0, still works), `spark.yarn.max.executor.failures` and
`spark.yarn.executor.failuresValidityInterval` (renamed in 3.5), the
`spark.launcher.childConectionTimeout` typo (3.2), `spark.executor.plugins` (removed in 3.0, use
`spark.plugins`) and `spark.sql.hive.verifyPartitionPath` (removed in 4.0, use
`spark.files.ignoreMissingFiles`). Any other `spark.*` literal containing `.blacklist.` or ending
in `.blacklist` is reported by a fuzzy fallback. Key fragments (pieces of an interpolation with
arguments, operands of `+`, literals ending in `.`) are skipped. Warn only, because each
replacement key needs a minimum Spark version on the cluster.

### ColumnExprRemovedWarn (lint)

Flags `column.expr` and `new Column(expr: Expression)` / `new TypedColumn(expr, encoder)`, which
were removed in Spark 4.0 (Column now wraps a `ColumnNode`, SPARK-47933), including the
`extends Column(e)` form. The constructor overload is told apart by the static type of the
argument: `new Column("name")`, string concatenation, interpolations and `if`/`match` with
string branches are not flagged; an argument whose type cannot be resolved (for example a
type parameter such as `exprs.head`) is flagged only when the file imports
`catalyst.expressions` or the argument mentions `Expression`/`catalyst`. The message points at
`org.apache.spark.sql.classic.ColumnConversions` (for reading `.expr`) and
`ClassicConversions` (`Column(expr)`, only usable inside `org.apache.spark.*` packages). Warn
only: the replacement package does not exist before 4.0.

### DateTimePatternWarn (lint)

Inspects string-literal datetime patterns passed to `functions.date_format` / `from_unixtime`
(formatting), `to_date` / `to_timestamp` / `unix_timestamp` (parsing) and the `dateFormat` /
`timestampFormat` options of `DataFrameReader` / `DataStreamReader` (parsing) and
`DataFrameWriter` / `DataStreamWriter` (formatting). Text inside single quotes is skipped. It
reports week-based letters (`Y W w u e c`, rejected since 3.0), letters with no Spark 3
equivalent (`A B n N p`), `E F q Q` in parsing patterns, runs of 5+ `G/M/L/E/Q/q` or 7+ `y`,
and `F` in formatting patterns (SimpleDateFormat day-of-week-in-month became java.time
aligned-day-of-week-in-month). Interpolated or variable patterns are
skipped, and the `hh`-without-`a` heuristic is intentionally not implemented. The escape hatch
is `spark.sql.legacy.timeParserPolicy=LEGACY`.

### IsRunningLocallyWarn (lint)

Flags `TaskContext.isRunningLocally`, removed in Spark 3.0 together with local execution; the
guarded code path can usually be deleted.

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
