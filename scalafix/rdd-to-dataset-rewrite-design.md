# RDD → Dataset automatic rewrite — design

Status: **proposal / for review** (no rewrite code written yet).

This builds on the existing `RDDToDatasetMigrationCheck` lint rule. The goal is:

> Automatically rewrite an RDD pipeline to the typed `Dataset` API **iff** the
> whole file uses only migratable operations. If it uses any non-migratable
> operation, do not rewrite — just log (the current checker behaviour). Never
> emit code that fails to compile.

This mirrors the Databricks serverless-migration skill's *detection-first*
stance: detect statically, rewrite only the clearly-safe cases, and otherwise
surface structured guidance.

## Scope: Scala only

The rewrite is **Scala-only**. PySpark's `DataFrame` has no typed
`map`/`flatMap`/`reduce`, so there is no safe *mechanical* swap on the Python
side (those would require synthesising UDFs/column expressions from arbitrary
lambdas). PySparkler therefore stays detection/log-only (`PYRDD-DS-001`).

## Rule shape

- Keep `RDDToDatasetMigrationCheck` (lint-only) unchanged.
- Add a new **opt-in** rewrite rule `RDDToDatasetMigration` (`isRewrite = true`)
  that reuses the op classification (symbol-owner detection, `simpleOps`,
  `neutralOps`, blocking set).
- Whole-file gate: rewrite only if there are **zero** blocking/unknown RDD ops.
  Otherwise emit `RDDMigrationBlocked` lints and make **no** textual change.

## "Migratable" (checker) vs "auto-swappable" (rewrite)

The rewrite's safe set is a **subset** of the checker's `simpleOps`, because some
migratable ops are not a pure mechanical swap:

| RDD op | Dataset rewrite | Mechanical? |
|---|---|---|
| `map`, `filter`, `flatMap`, `mapPartitions`, `foreach`, `foreachPartition`, `distinct`, `reduce`, `count`, `collect`, `take`, `first`, `cache`, `persist`, `unpersist`, `coalesce`, `repartition`, `union` | identical method name on `Dataset` → **leave the call as-is** (at the base arity; `coalesce(n,shuffle)`/`distinct(n)`/`mapPartitions(f,preserves)` are blocked) | ✅ |
| `union`, `intersection` (binary) | also require the **argument** to trace to a convertible origin (else it stays an RDD) | ✅ when the operand traces |
| `intersection` | rename to `intersect` | ✅ |
| `isEmpty`, `sample`, `toLocalIterator`, `checkpoint`, `subtract` | Dataset namesake differs even at the base arity (parameterless `isEmpty`; different sampler; `java.util.Iterator`; returns a new Dataset; `subtract` is a left-anti join) | ❌ → block (log) |
| `++` | would rename to `union`, but not yet handled | ❌ → block (log) |
| `sortBy(f)` | `orderBy`/`sort` need a **Column**, not a `T => K` function | ❌ → disqualifies auto-rewrite (log) |

So the chain itself usually stays textually the same; only the **origin** changes
type (RDD→Dataset) and a few ops get renamed.

### Accessors that don't exist on `Dataset`

The checker treats RDD accessors (`getNumPartitions`, `sparkContext`,
`partitions`, `partitioner`, `dependencies`, `toDebugString`,
`getStorageLevel`, `preferredLocations`, `name`, `setName`, `id`) as *neutral*
(ignored). They do **not** exist on `Dataset` (you'd go via `.rdd`). For the
rewrite they must either be routed through `.rdd` or **disqualify** the file.
MVP: **disqualify (log)** if any such accessor is used on the converted value.

## The three parts of a pipeline

### 1. Origin (how the RDD is created)

Supported (rewrite):

| Origin | Rewrite |
|---|---|
| `sc.parallelize(seq)` / `sc.makeRDD(seq)` | `spark.createDataset(seq)` |
| `sc.parallelize(seq, n)` | `spark.createDataset(seq).repartition(n)` (preserve partitioning) — *open question* |
| `<dataset>.rdd` (a typed `Dataset[T]`) | drop `.rdd`, use the Dataset directly |
| `sc.textFile(path)` | `spark.read.textFile(path)` (`Dataset[String]`) — *phase 2* |

Unsupported → **log, no rewrite**: RDD as a method parameter or return type
(can't change signatures), `sc.wholeTextFiles`/`sequenceFile`/`objectFile`/
`hadoopFile`, `sc.emptyRDD`, `sc.union`, custom RDDs, `DataFrame.rdd` (the `Row`
element type has no implicit encoder), or any origin we can't recognise.

### 2. Operations

Leave name-identical ops untouched; apply the renames above. `sortBy` and any
non-Dataset accessor disqualify the file.

### 3. Exit (how the result is consumed)

Converting the value from `RDD[T]` to `Dataset[T]` is only safe if the value is
**not consumed as an RDD** elsewhere (passed to an `RDD`-typed parameter,
returned from an `RDD`-typed method, etc.). MVP: if the converted value flows
into an RDD-typed context, **skip (log)**. (Phase 2 could insert `.rdd` at the
boundary.)

## Encoders / SparkSession — the key precondition

Typed `Dataset` ops and `createDataset` need an `Encoder[T]`, normally from
`import spark.implicits._`, where `spark` is a `SparkSession`. So the rewrite:

1. Must find a `SparkSession` in scope (a `val spark: SparkSession`, a
   `SparkSession.builder()...getOrCreate()`, or derivable from the `sc` used in
   the origin). If none is identifiable → **skip (log)**.
2. Must ensure `import spark.implicits._` is present; add it if missing.
3. Cannot fully verify statically that `Encoder[T]` exists for the element type.
   Mitigation: rely on the implicits import (covers primitives, `String`,
   tuples, case classes, `Product`); accept that exotic element types may still
   fail to compile, and document this. (This is why the rule is opt-in and the
   migration is validated by recompiling — the project's stated workflow.)

## Safety preconditions (rewrite only if ALL hold; else log)

1. File has zero blocking/unknown RDD ops (whole-file gate).
2. Every RDD op is in the auto-swappable set (rename map known); no `sortBy`, no
   non-Dataset accessor.
3. The origin is a supported, convertible form.
4. A `SparkSession` is identifiable in scope.
5. The converted value is not consumed as an RDD elsewhere.

## Examples

```scala
// 1) parallelize + map/filter + action
// before
val rdd = sc.parallelize(Seq(1, 2, 3))
val out = rdd.map(_ + 1).filter(_ % 2 == 0).collect()
// after  (import spark.implicits._ added if missing)
val rdd = spark.createDataset(Seq(1, 2, 3))
val out = rdd.map(_ + 1).filter(_ % 2 == 0).collect()

// 2) drop .rdd on an existing Dataset
// before
val names = ds.rdd.map(_.name)
// after
val names = ds.map(_.name)

// 3) op rename (intersection), union kept (both operands trace to a Dataset)
// before
val r = a.intersection(b).union(c)       // a,b,c: RDDs from convertible origins
// after
val r = a.intersect(b).union(c)          // a,b,c: Datasets
// note: subtract is NOT rewritten — it is a left-anti join, blocked + logged

// 4) blocked → unchanged + lint
val r = rdd.map(_ + 1).reduceByKey(_ + _)   // reduceByKey blocks → no rewrite
```

## Phasing

- **Phase 1 (implemented):** origins `sc.parallelize`/`makeRDD` →
  `createDataset`, `sc.textFile` → `read.textFile`, and `dataset.rdd` drop;
  name-identical ops only; SparkSession from `<x>.sparkContext` else `spark`;
  implicits must already be present. Disqualify (log, no change) on renames
  (`++`/`intersection`/`subtract`), `sortBy`, non-Dataset accessors, blocking
  ops, or any non-swap op.
- **Phase 2:** renames (`++`→`union`, `intersection`→`intersect`,
  `subtract`→`except`) gated by receiver tracing; `import implicits._`
  synthesis; richer SparkSession discovery; insert `.rdd` at RDD-typed exits.
- **Phase 3 (maybe never):** `sortBy` → `orderBy` with column synthesis.

## Testing

scalafix testkit input/output fixture pairs (rewrites compare against `output/`):

- `parallelize` + `map`/`filter` + `collect` → `createDataset` version.
- `ds.rdd.map(...)` → `ds.map(...)`.
- `intersection`/`subtract`/`++` renames.
- blocked file (`reduceByKey`) → unchanged (input-only, lint asserted).
- unsupported origin (RDD method parameter) → unchanged (input-only, lint).

## Decisions (resolved)

1. `sc.parallelize(seq, n)` → `createDataset(seq).repartition(n)` (preserve `n`).
2. SparkSession sourcing is **best-effort**: taken from `<x>.sparkContext` when
   the origin is written that way, else from the session whose `implicits._` are
   imported (the encoders and `createDataset` come from the same session; falls
   back to `spark` only if neither is determinable). The
   `import <session>.implicits._` is **not** synthesised (a local import can't be
   placed reliably) — it must already be in scope, otherwise the rewritten file
   won't compile and the user fixes it up. This is the "best-effort" tradeoff.
3. `sortBy` (and `intersection`/`subtract`/`++`) are **not** auto-swapped in
   Phase 1 — they're logged as "migrate manually". They need a renamed call
   and/or a column expression, which is unsafe without receiver tracing.
4. Implemented as a **separate opt-in rule** `RDDToDatasetMigration`
   (`isRewrite = true`); the lint-only `RDDToDatasetMigrationCheck` is unchanged.

## Status

**Implemented** (`RDDToDatasetMigration`), and deliberately **conservative** — it
rewrites only when the result is guaranteed to compile to the same thing and
compute the same result. Three rounds of adversarial self-review showed the
"name-identical op = safe swap" premise has many sharp edges (signature, arity,
return-type, partitioning, equality, and AST-shape differences), so the safe
surface was narrowed and everything uncertain is logged, not rewritten.

- Whole-file gate: rewrite only if every RDD op is in the audited safe set;
  otherwise log the first category of blockers and change nothing.
- Origins (single-argument forms only): `<sess>.sparkContext.parallelize(seq)` /
  `makeRDD(seq)` → `<sess>.createDataset(seq)`, `textFile(path)` →
  `read.textFile(path)`, `.rdd` drop. `parallelize(seq, n)` / `textFile(path, n)`
  are **blocked** (a `.repartition(n)` shuffle would reorder rows / the hint has
  no Dataset analog).
- Rename: `intersection` → `intersect` (both `INTERSECT DISTINCT`). `subtract` is
  **blocked** — RDD removes every row whose value is in the other RDD (a
  left-anti join); neither `except` (`EXCEPT DISTINCT`) nor `exceptAll`
  (`EXCEPT ALL`, multiset) reproduces it.
- Binary ops (`union`, `intersection`): the **argument** must also `tracesToDataset`
  (an origin call, an RDD-op chain on one, or a local `val` bound to one), else it
  would remain an RDD and the call wouldn't compile.
- Arity-aware: `coalesce(n, shuffle)`, `distinct(n)`, `mapPartitions(f, preserves)`
  are blocked (no Dataset overload), including the explicit-type-argument form
  `mapPartitions[U](f, true)` (collected via `Term.ApplyType`).
- Blocked because the Dataset namesake differs even at the base arity: `isEmpty`
  (parameterless on Dataset), `sample` (different sampler/seed), `toLocalIterator`
  (`java.util.Iterator`), `checkpoint` (returns a new Dataset).
- Blocked: any op used as an eta-expanded method value (`rdd.map _`); any file
  declaring an `RDD[...]` type (the annotation would be left dangling); more than
  one `implicits._` import (ambiguous session).
- Encoders: requires an `import <session>.implicits._` in scope (not synthesised);
  logged (`RDDMigrationNeedsImplicits`) if missing.

Each blocker/limitation above has a dedicated test fixture (`RDDToDatasetMigration*`).

Accepted, documented limitations (still rewritten): `parallelize(seq)` /
`textFile(path)` yield a different partition *count* than the RDD (per-row results
identical; partition-count-sensitive side effects differ), and `distinct`/`intersect`
dedup on the encoded representation, not a custom `equals`.

Not done (future): `++` → `union`, inserting `.rdd` at RDD-typed exits, `sortBy` →
`orderBy`. PySpark stays detect/log-only (a best-effort, name-based heuristic whose
"migratable" hint is advisory — its blocklist can't be exhaustive without type
information, unlike the symbol-resolved Scala check).
