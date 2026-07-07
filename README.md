Aims to help people upgrade to the latest version of Spark.

# Tools

## Upgrade Validation

While we all wish we had a great test suite that covered all of the possible issues, that is not the case for all of our pipelines.

### Side-By-Side Comparison

To support migrations for pipelines with incomplete test coverage, we have tooling to compare two runs of the same pipeline on different versions of Spark.
Right now it requires that you specify the table to be compared, but (not yet started) for Iceberg tables we plan to provide a custom Iceberg library which automates this component.

### Performance only Comparison (not yet started)

## Semi-Automatic Upgrades

Upgrading your code to a new version of Spark is perhaps not how most folks wish to spend their work day (let alone their after work day). Some parts of the migrations can be automated, and when combined with the upgrade validation described above can (hopefully) lead to reasonably confident automatic upgrades.

### SQL (WIP)

Spark SQL has some important changes between Spark 2.4 and 3.0 as well as some smaller changes in between later versions. (Spark SQL migration guide)[https://spark.apache.org/docs/3.3.0/sql-migration-guide.html] covers most of the expected required changes.

The SQL migration tool is built using (SQLFluff)[https://sqlfluff.com/], which has a (Spark SQL dialect)[https://docs.sqlfluff.com/en/stable/dialects.html].

#### Limitations / Unique Challenges

Out of the box SQLFluff lacks access to type information that is available when migrating Scala code, and the AST parser is not a 1:1 match with the underlying parser used by Spark SQL. A potential mitigation (if we end up needing type information) is integrating with Spark SQL to run an EXPLAIN on the input query and extract type information.


Some migration rules are too much work to fully automate so instead output warnings for users to manually verify.

We do not have an equivelent to "Scala Steward" for SQL files and SQL can target multiple backends. In most situations, the scheduler job type can be used to determine the engine.

### PySpark (Python Spark) Upgrade (WIP)

The PySpark Upgrade tool - PySparkler - is currently built using [LibCST](https://github.com/Instagram/LibCST).
More on the tool's design and challenges can be found in the subdirectory README of the tool [here](./pysparkler/README.md).

It includes rules for upgrading through Spark 4.x (3.5→4.0, 4.0→4.1, and the 4.1→4.2 preview). See the
[PySparkler README](./pysparkler/README.md#pyspark-upgrades-supported) for the full list of supported migrations and
the current gaps.

### Scala Upgrade (WIP)

The Scala upgrade tooling is built on top of ScalaFix and has access to (most) of the type information. Spark's Scala APIs are perhaps the fastest changing of three primary languages used with Spark.

For Spark 4.x, the biggest source-level changes for Scala pipelines are behavior changes (ANSI mode on by default, Apache Mesos removal, and many config default changes) rather than API signature changes. These are covered by the `AnsiModeEnabledWarn` and `MesosRemovedWarn` lint rules, plus the legacy-config map in [`conf_migrate`](./conf_migrate/migrate.py). See the [scalafix README](./scalafix/readme.md#spark-4x-rules) for details.


#### Limitations / Unique Challenges

While scalafix can be integrated with tools like Scala Steward (yay!), recompiling and publishing new artifacts is required to verify the changes. It is likely that dependencies will need to be manually upgraded.
