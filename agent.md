# Validating a Spark upgrade in WAP mode (guide for coding agents)

Use this when asked to help upgrade a Spark pipeline. The flow is:

1. Run the static migration tools over the pipeline's source and review their output.
2. Build the pipeline twice: on the current Spark ("control") and on the target Spark ("new").
3. Run both through `pipelinecompare/domagic.py --iceberg`. Each run stages an Iceberg
   write-audit-publish (WAP) snapshot that readers never see; the tool diffs the two snapshots.
4. If they match, the upgrade is ready to promote. If not, inspect the diff and loop.

`e2e_demo/scala/run_demo.sh` (sbt) and `run_demo-gradle.sh` run this whole flow in CI and are the
ground truth for flags and ordering. `NO_PROMPT=yes ./run_demo.sh` runs it non-interactively.

## 1. Static migration

- Scala: add the scalafix plugin and rules as in `docs/scala/sbt.md` or `docs/scala/gradle.md`, copy
  `scalafix/.scalafix.conf` into the project and run `sbt scalafix` for rewrites, then
  `scalafix/.scalafix-warn.conf` for lint-only warnings.
- Python: `pysparkler upgrade --input-file job.py --dry-run`, then without `--dry-run`.
- SQL: `pip install <spark-upgrade>/sql` and `sqlfluff fix --dialect sparksql query.sql`.
- Configs that restore old behaviour when a rewrite is not possible: `conf_migrate/migrate.py`.

Treat every warning and `# PySparkler:` hint as a to-do for the human. Dependency bumps (Spark,
Scala, Iceberg runtime, test libraries) are not automated.

## 2. Build control and new artifacts

The tool derives the new jar name from the control one by inserting a suffix after the artifact
name: with `--new-jar-suffix "-3"`, `app_2.12-0.0.1.jar` becomes `app-3_2.12-0.0.1.jar`. Name the
new artifact accordingly. You also need two Spark distributions, each with a matching Iceberg Spark
runtime jar in `jars/` (see `e2e_demo/scala/dl_dependencies.sh`).

## 3. WAP comparison

### How it works

`domagic.py --iceberg` runs the new pipeline, waits 10 seconds, then runs the control pipeline. It
injects this into each command at the `{spark_extra_conf}` placeholder:

```
--driver-java-options "-javaagent:<repo>/iceberg-spark-upgrade-wap-plugin/target/scala-2.12/<jar>" --conf spark.wap.id=<42|43>
```

The agent registers an Iceberg listener that prints one line to driver stderr per commit:

```
IcebergListener: Created snapshot 6723816932461398207 on table local.db.t summary {...} from operation append
```

`domagic.py` collects `table@snapshot` pairs from each run's stderr, pairs control and new **by
position**, and runs `table_compare.py` with `--spark-command`. That job fails on a schema mismatch,
then diffs the rows with `subtract` in both directions (grouping by all columns if counts differ)
and fails if more than `--row-diff-tolerance` (default 0) of the rows differ. It tries an Iceberg
changelog view first, but that path currently always fails and falls back to reading both
snapshots in full, which is logged and expected.

### Prerequisites

- The pipeline writes through Iceberg and runs in **client or local mode** (the agent is a local
  path passed via `--driver-java-options`), and does not redirect driver stderr.
- Every output table exists with `write.wap.enabled=true`. Setting that on a production table is a
  metadata change; ask the human first.
- JDK 11 for Spark 2.x/3.x (Spark 2.4 does not start on 17), JDK 17+ for Spark 4.
- `pip install colorama` for the Python running `domagic.py` and for the one `spark-submit` uses.
- The WAP plugin is built for Scala 2.12. For a Scala 2.13 Spark (all of Spark 4), rebuild it with
  `scalaVersion` set to that Spark's 2.13 release.

### Run it

```bash
cd <spark-upgrade>/iceberg-spark-upgrade-wap-plugin && sbt clean test package
```

Run `domagic.py` from `pipelinecompare/`, since it refers to its helper scripts by relative path:

```bash
cd <spark-upgrade>/pipelinecompare
python domagic.py --iceberg \
  --spark-control-command /path/to/spark-2.4.8/bin/spark-submit \
  --spark-new-command     /path/to/spark-3.3.1/bin/spark-submit \
  --spark-command         /path/to/spark-3.3.1/bin/spark-submit \
  --new-jar-suffix "-3" \
  --warehouse-config "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog ..." \
  --combined-pipeline "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog ... \
    --class com.example.Main /jars/app_2.12-0.0.1.jar <args>"
```

| Flag | Meaning |
|---|---|
| `--spark-control-command`, `--spark-new-command` | `spark-submit` of the old and new Spark. |
| `--spark-command` | `spark-submit` for the comparison job. Use the newer Spark (Iceberg 1.2+ procedures). |
| `--combined-pipeline` | Arguments after `spark-submit`. `{spark_extra_conf}` is inserted before the first option such as `--conf` or `--class` (or the first `.jar`) unless you place it yourself. |
| `--control-pipeline`, `--new-pipeline` | Use instead of `--combined-pipeline` when the two runs differ; both need `{spark_extra_conf}`. |
| `--warehouse-config` | Catalog arguments for the comparison job only. |
| `--row-diff-tolerance`, `--compare-precision` | Allowed fraction of differing rows; rounding for fractional columns. |
| `--output-tables` | Fills `{output_tables}`. In WAP mode it does **not** filter which tables are compared. |

### Reading the output

- `Huzzah! ctrl: [...] new: [...]` lists the snapshots found. **Empty lists mean no Iceberg commit
  was seen** (not Iceberg, cluster mode, or stderr redirected), and the comparison then passes
  vacuously. Never report that as a pass.
- Lists of different lengths or orders mean the positional pairing is wrong; make both runs write
  tables in the same order, or compare one table per run.
- `Error running contorl pipeline` / `Error running new pipeline` (sic) means a run failed; its
  stdout and stderr follow.
- `Control schema and target schema do not match` plus two schemas: the upgrade changed a type or
  nullability. Decide with the human whether that is acceptable.
- `Found N that were not in the control` / `Found N missing from new new pipeline` plus sample rows:
  a real data difference. ANSI and datetime behaviour changes are the usual causes.
- `Table compare status: ok`: passed within the tolerance.

### Cleanup

The staged snapshots stay in table metadata and hold data files; `domagic.py` never removes them.
Expire them with Iceberg's `expire_snapshots` procedure when done. To publish the new run's result,
`CALL <catalog>.system.cherrypick_snapshot('db.t', <snapshot_id>)`. Only do either when the human
asks.

## Runbook

1. Confirm source and target Spark versions, Scala version, build tool, which output tables are
   Iceberg, and the catalog configuration.
2. Run the static tools on a branch and summarise every rewrite and warning.
3. Build both artifacts with distinguishable names and smoke-test both.
4. Build the WAP plugin and make sure every output table has `write.wap.enabled=true`.
5. Run `domagic.py --iceberg` from `pipelinecompare/`, in client mode.
6. Report the snapshot ids compared, the comparison command, and pass or fail with the schema diff
   or sample rows. Leave snapshot cleanup and publishing to the human.
