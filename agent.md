# spark-upgrade for coding agents: validating a Spark upgrade in WAP mode

This file is written for an AI coding agent (Claude Code, Codex, Cursor, ...) that has been
asked to help upgrade an Apache Spark pipeline to a newer Spark version. It explains how to use
the tools in this repository, and in particular how to run the **WAP (write-audit-publish)
side-by-side comparison** that proves the upgraded pipeline produces the same output as the
old one without touching the production table.

Read this whole file before running anything. The commands here shell out to `spark-submit`
and write to Iceberg tables; a wrong flag can leave stray snapshots behind.

## The upgrade workflow in one paragraph

1. Run the **static migration tools** over the pipeline's source (`scalafix` rules for
   Scala, `pysparkler` for Python, the `sqlfluff` plugin for SQL) and review the rewrites and
   warnings they produce.
2. Build the pipeline **twice**: once on the current Spark version ("control") and once on the
   target version ("new"), with artifact names you can tell apart.
3. Enable **Iceberg WAP** on the pipeline's output tables, then run both builds through
   `pipelinecompare/domagic.py --iceberg`. Each run writes a *staged* Iceberg snapshot that is
   not visible to readers; the tool compares the two snapshots and fails if they differ.
4. If the comparison passes, promote the upgrade. If it fails, inspect the row diff, decide
   whether it is a real regression or an acceptable change, and loop.

Steps 1 and 2 are covered briefly; step 3 is the bulk of this document.

## Repository map (only what you need)

| Path | What it is |
|---|---|
| `scalafix/` | Scalafix rules that rewrite / lint Scala Spark code. `.scalafix.conf` lists the rewrite rules, `.scalafix-warn.conf` the lint-only rules. See `scalafix/readme.md` and `docs/scala/{sbt,gradle}.md`. |
| `pysparkler/` | `pysparkler upgrade --input-file script.py` rewrites PySpark scripts/notebooks and leaves `# PySparkler:` code hints. See `pysparkler/README.md`. |
| `sql/` | A sqlfluff plugin (`sqlfluff fix --dialect sparksql`) for Spark SQL migrations. |
| `conf_migrate/migrate.py` | A map of `spark.sql.legacy.*`-style configs that restore pre-upgrade behaviour when a rewrite is not possible. |
| `iceberg-spark-upgrade-wap-plugin/` | A tiny Java agent (`-javaagent:`) that registers an Iceberg listener and prints every snapshot the pipeline creates. The comparison tool reads those lines. |
| `pipelinecompare/domagic.py` | The driver: runs control and new pipelines, collects the snapshots, launches the comparison. |
| `pipelinecompare/table_compare.py` | The PySpark job that diffs two snapshots of a table. |
| `e2e_demo/scala/run_demo.sh` | The reference end-to-end run (Spark 2.4.8 to 3.3.1, sbt). `run_demo-gradle.sh` is the Gradle variant. CI runs both, so they are the ground truth for flags and ordering. |

## Step 1: static migration (short version)

Scala (sbt):

```bash
# in the pipeline's project, after adding the scalafix plugin + rules dependency as described in docs/scala/sbt.md
cp <spark-upgrade>/scalafix/.scalafix.conf ./.scalafix.conf
sbt scalafix                       # rewrites
cp <spark-upgrade>/scalafix/.scalafix-warn.conf ./.scalafix.conf
sbt scalafix                       # lint-only warnings; non-zero exit means warnings were found
```

Python:

```bash
pipx install pysparkler
pysparkler upgrade --input-file path/to/job.py --dry-run     # prints a unified diff
pysparkler upgrade --input-file path/to/job.py               # rewrites in place, adds code hints
```

SQL:

```bash
pip install <spark-upgrade>/sql
sqlfluff fix --dialect sparksql path/to/query.sql
```

Treat every `Warn` rule and every `# PySparkler:` hint as a to-do item for the human; the tools
deliberately do not auto-rewrite anything whose semantics they cannot prove. Recompile after
the rewrites. Dependency bumps (Spark itself, `spark-testing-base`, Iceberg runtime, Scala
version) are **not** automated.

## Step 2: build control and new artifacts

Produce two jars (or two wheels / two script trees) that only differ in the Spark version and
the migration rewrites. The comparison tool tells them apart by a suffix in the artifact name:
the demo appends `-3` to the project name, so `sparkdemoproject_2.12-0.0.1.jar` becomes
`sparkdemoproject-3_2.12-0.0.1.jar`. Keep both artifacts on the machine that will run the
comparison (the demo copies them to `/tmp/spark-migration-jars`).

Also make sure you have **two Spark distributions** on disk, one per version, each with the
matching **Iceberg Spark runtime jar** in its `jars/` directory. `e2e_demo/scala/dl_dependencies.sh`
shows the pairing used by the demo (`iceberg-spark-runtime-2.4` 1.1.0 for Spark 2.4.8,
`iceberg-spark-runtime-3.3_2.12` 1.3.0 for Spark 3.3.1).

## Step 3: WAP comparison

### How it works

Iceberg's write-audit-publish support stages a write as a snapshot that is **not** the table's
current snapshot. With the table property `write.wap.enabled=true` and a `spark.wap.id` set on
the writing session, every commit the pipeline makes is staged instead of published, so the
production table's readers never see it.

`domagic.py --iceberg` runs the two pipelines one after the other (new first, then control, with a
10 second gap so that table creation does not race). Each pipeline gets two things injected into
its `spark-submit` command through the `{spark_extra_conf}` placeholder:

```
--driver-java-options "-javaagent:<repo>/iceberg-spark-upgrade-wap-plugin/target/scala-2.12/<plugin>.jar" --conf spark.wap.id=<42|43>
```

The agent (`Agent.premain`) calls `Listeners.register(WAPIcebergListener, classOf[CreateSnapshotEvent])`
inside the driver JVM. Every Iceberg commit then prints one line to **stderr**:

```
IcebergListener: Created snapshot 6723816932461398207 on table local.newest_farttable summary {...} from operation append
```

`domagic.py` captures each pipeline's stderr, extracts `<table>@<snapshot_id>` pairs with the
regex `^IcebergListener: Created snapshot (\d+) on table (.+?) summary .+`, pairs the control
list with the new list **by position**, and launches `table_compare.py` with the newer
`spark-submit`:

```
<spark-command> <warehouse-config> --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    table_compare.py --control-tables db.t@<ctrl_snapshot> --target-tables db.t@<new_snapshot> [--row-diff-tolerance X] [--compare-precision N]
```

`table_compare.py` first tries to build Iceberg changelog views from the snapshots' common
ancestor (the "CDC" path) and, if that fails, reads both snapshots in full with
`spark.read.option("snapshot-id", ...)`. In the current code the CDC helper
(`spark_utils.drop_iceberg_internal_columns`) raises on every call, so **expect the full
snapshot comparison** every time; the CDC attempt is only an optimisation and its failure is
logged, not fatal. The comparison then:

1. fails if the two schemas differ (both schemas are printed);
2. optionally rounds fractional columns to `--compare-precision` digits;
3. computes `control.subtract(target)` and `target.subtract(control)` (falling back to casting
   every column to string for types `subtract` cannot handle);
4. fails if the number of differing rows exceeds `--row-diff-tolerance` (a fraction of the
   control row count, default `0.0`, i.e. exact match);
5. if the row counts still differ (duplicates), repeats the diff on `groupBy(all columns).count()`.

Success prints `Table compare status: ok` and `domagic.py` exits 0. Any difference prints the
offending rows (`Found N that were not in the control`, `Found N missing from new new pipeline`),
then `DANGER DANGER: Error during table compare!`, and the exit code is non-zero.

### Prerequisites checklist

- **Java 11** for Spark 2.x/3.x era builds (Spark 2.4.8 does not start on JDK 17). Spark 3.3+
  also runs on 17; Spark 4.x needs 17+.
- **sbt** to build the WAP plugin, and the two **Spark distributions** with Iceberg runtime jars
  (see Step 2).
- **Python 3.9+** on the machine running `domagic.py`, with `pip install colorama`
  (`pipelinecompare/requirements.txt` lists `iceberg lakefs_client colorama`; only `colorama`
  is needed for Iceberg WAP mode, `lakefs_client` is for `--lakeFS`).
- The Python used by `spark-submit` for `table_compare.py` (`PYSPARK_PYTHON`) must also be able
  to `import colorama` and `pyspark`.
- The pipeline must **write through Iceberg** (DataFrame/SQL writes to an Iceberg catalog table).
  Non-Iceberg writes produce no snapshot events and the comparison has nothing to compare.
- The pipeline must run in **client mode** (or local mode). The java agent path is a local file
  on the driver and is passed via `--driver-java-options`; in cluster mode the driver runs
  elsewhere and never loads it.
- Do not let the pipeline swallow or redirect driver **stderr**: the listener prints there and
  `domagic.py` parses it. A log4j config that sends `System.err` elsewhere breaks detection.
- The WAP plugin is built for **Scala 2.12** (`ThisBuild / scalaVersion` in its `build.sbt`). It
  only touches Iceberg's listener API, so it works with any Spark that bundles Scala 2.12. For a
  Scala 2.13 Spark (Spark 4.x), rebuild it with `scalaVersion` set to the 2.13 release your Spark
  ships.
- Output tables must **exist before the run** with `write.wap.enabled=true`. Create them or
  alter them, e.g. `ALTER TABLE db.t SET TBLPROPERTIES ('write.wap.enabled'='true')`. This is a
  table metadata change on the production table; it does not alter data and readers are
  unaffected, but tell the human before doing it.

### Build the WAP plugin

```bash
cd <spark-upgrade>/iceberg-spark-upgrade-wap-plugin
sbt clean test package      # tests start a local Spark with Iceberg and assert the listener fires
ls target/scala-2.12/*.jar  # domagic.py globs this directory and picks the newest jar
```

### Run the comparison

Run `domagic.py` **from the `pipelinecompare` directory**: it refers to `table_compare.py` by
relative path and imports `utils.py` and `spark_utils.py` from the current directory.

The simplest form gives one `spark-submit` argument string and lets the tool derive the two
commands (this is what the e2e demo does):

```bash
cd <spark-upgrade>/pipelinecompare
python domagic.py --iceberg \
  --spark-control-command /path/to/spark-2.4.8/bin/spark-submit \
  --spark-new-command     /path/to/spark-3.3.1/bin/spark-submit \
  --spark-command         /path/to/spark-3.3.1/bin/spark-submit \
  --new-jar-suffix "-3" \
  --warehouse-config " \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse" \
  --combined-pipeline " \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
    --class com.holdenkarau.sparkDemoProject.CountingLocalApp \
    /tmp/spark-migration-jars/sparkdemoproject_2.12-0.0.1.jar input.txt local.newest_farttable"
```

What each flag does:

| Flag | Meaning |
|---|---|
| `--iceberg` | Use Iceberg WAP mode (the other modes are `--iceberg-legacy`, which copies tables, and `--lakeFS`). |
| `--spark-control-command` / `--spark-new-command` | The `spark-submit` of the old and new Spark. Each is prepended to the pipeline arguments. |
| `--spark-command` | The `spark-submit` used for `table_compare.py`. Use the **newer** Spark: the comparison needs an Iceberg runtime that supports `ancestors_of` and `create_changelog_view` (Iceberg 1.2+). |
| `--combined-pipeline` | Everything after `spark-submit` for the control run. The tool inserts `{spark_extra_conf}` before the first `--conf`/`--class`/`--jars`/`.jar` token if you did not place the placeholder yourself. |
| `--new-jar-suffix` | How to derive the new run's jar name: the regex `([ ,])([\-_\w/]+?)([\-_.0-9]+)\.jar` splits `<name><version>.jar` and inserts the suffix after `<name>`, so `app_2.12-0.0.1.jar` becomes `app-3_2.12-0.0.1.jar`. Name your new artifact accordingly. |
| `--warehouse-config` | Extra `spark-submit` arguments (catalog configuration) for the comparison job only. Whitespace separated. |
| `--row-diff-tolerance` | Allowed fraction of differing rows, `0.0` by default. |
| `--compare-precision` | Round fractional columns to this many digits before comparing. |
| `--output-tables` | Substituted into `{output_tables}` in the pipeline command (comma separated). The help text says it filters the compared tables, but in WAP mode it currently does **not** filter; every table the listener reported is compared. |
| `--no-cleanup` | Accepted for symmetry with the other modes; WAP mode never deletes anything anyway. |

If the two runs need genuinely different command lines (different main class, different
arguments), use `--control-pipeline` and `--new-pipeline` instead of `--combined-pipeline`.
Both are passed through `bash -c`, and both must contain `{spark_extra_conf}` where the
java-agent and WAP id options should go; `{output_tables}`/`{input_tables}` are optional
placeholders. You cannot mix the two styles.

### Reading the output

- `Huzzah! ctrl: ['db.t@<id>'] new: ['db.t@<id>'] :D` lists the snapshots found. **Empty lists
  mean no Iceberg commit was observed**: check that the pipeline writes to Iceberg, runs in
  client mode, and that stderr was not redirected. The comparison then runs with no tables and
  passes vacuously, so never report success on empty lists.
- If the two lists have different lengths or the tables appear in a different order, the
  positional pairing is wrong. Make the pipelines write tables in the same order, or compare one
  output table per run.
- `Error running contorl pipeline` / `Error running new pipeline` (sic) prints the failing
  command plus its stdout and stderr, then raises `Error running pipelines.`. Fix the pipeline,
  not the comparison.
- `Control schema and target schema do not match` followed by two `printSchema` dumps: the
  upgrade changed a column type or nullability. Common for Spark 3.0+ (e.g. `extract(second ...)`
  returning decimal, `CHAR` becoming `STRING`); decide with the human whether the new schema is
  acceptable, then adjust the pipeline or the downstream consumers.
- `Found N that were not in the control` / `Found N missing from new new pipeline` with sample
  rows: a real data difference. Compare the rows; Spark 3.0's ANSI/date-time changes and
  Spark 4.0's ANSI-on-by-default are the usual suspects. `--compare-precision` and
  `--row-diff-tolerance` exist for floating point noise and for inputs that legitimately change
  between runs (the demo's second run reads `/var/log/syslog` to show an expected failure).
- `Warning converting all to strings....`: `subtract` could not handle a column type (maps,
  for example); the comparison continued on string-cast columns. Fine, but slower and less strict.
- `Table compare status: ok` and `Running [...] was a success! All signs point towards prod.`:
  the run passed with the given tolerance.

### Cleaning up and publishing

The staged WAP snapshots stay in the table metadata. They are not ancestors of the current
snapshot, so normal readers never see them, but they do hold data files. After you are done:

- To discard them, let Iceberg's snapshot expiration remove them, e.g.
  `CALL local.system.expire_snapshots(table => 'db.t', older_than => TIMESTAMP '...')`, then
  `CALL local.system.remove_orphan_files(table => 'db.t')` if needed. Follow the Iceberg docs for
  your version; the procedures need `IcebergSparkSessionExtensions`.
- To keep the **new** pipeline's result as the table's current state (i.e. actually publish the
  audited write), cherry-pick its snapshot: `CALL local.system.cherrypick_snapshot('db.t', <new_snapshot_id>)`
  (or `publish_changes` with the WAP id on Iceberg versions that have it). Only do this when the
  human explicitly asks; the whole point of WAP mode is that the comparison itself publishes nothing.
- `domagic.py` prints `Done!. You may want to cleanup snapshots.` as a reminder; it never
  expires snapshots itself.

### The e2e demo as a template

`e2e_demo/scala/run_demo.sh` (sbt) and `run_demo-gradle.sh` (Gradle) run the whole flow against a
word-count job and are executed in CI on every pull request. When in doubt about flags, ordering,
or catalog configuration, copy from them. `NO_PROMPT=yes ./run_demo.sh` runs non-interactively,
downloads about 830 MB of Spark/Hadoop archives on first use, and takes 10 to 20 minutes.

## Runbook for an agent

1. Confirm with the human: source and target Spark versions, Scala version, build tool, which
   output tables are Iceberg, and which Iceberg catalog configuration the pipeline uses.
2. Run the static tools (Step 1) on a branch, summarise every rewrite and warning, and get the
   human to approve the dependency bumps you cannot automate.
3. Build control and new artifacts with distinguishable names (Step 2). Verify both start with
   `--help`-style smoke runs or unit tests before comparing.
4. Build the WAP plugin (`sbt clean test package`) and confirm the jar exists under
   `iceberg-spark-upgrade-wap-plugin/target/scala-2.12/`.
5. Make sure every output table has `write.wap.enabled=true`; ask before altering production tables.
6. Run `domagic.py --iceberg ...` from `pipelinecompare/`, in client mode, with the newer
   `spark-submit` as `--spark-command`.
7. Report: the snapshot ids compared, the exact comparison command, pass/fail, and, on failure,
   the schema diff or sample rows. Never summarise an empty snapshot list as a pass.
8. Leave snapshot cleanup and any `cherrypick_snapshot` publish to an explicit human decision.
