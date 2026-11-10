# NOTE: this dict is flat and version-agnostic: it is the union of every "set X to restore the
# old behaviour" hint from the Spark 3.0 .. 4.2 migration guides. Two keys have a different
# restore value depending on the Spark version you are coming FROM:
#
#   * spark.sql.legacy.timeParserPolicy   -- a 2.4 source needs LEGACY (SimpleDateFormat
#     semantics); the EXCEPTION below only restores the 3.x behaviour for a 3.x -> 4.0 upgrade.
#   * spark.sql.legacy.ctePrecedencePolicy -- a 2.4 source needs LEGACY (inner CTE wins); the
#     EXCEPTION below only restores the 3.x behaviour for a 3.x -> 4.0 upgrade.
#
# Users upgrading from Spark 2.4 must override both keys to LEGACY after running this tool.
# Also note that spark.sql.legacy.sessionInitWithConfigDefaults is a *static* conf: it only
# takes effect in spark-defaults.conf / on SparkSession construction, not via SET.
legacy_apped_rules = {
    # ------------------------------------------------------------------
    # Spark 3.0 (2.4 -> 3.0)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-24-to-30
    "spark.sql.storeAssignmentPolicy": "Legacy",
    "spark.sql.legacy.setCommandRejectsSparkCoreConfs": "false",
    # Dataset.groupByKey on a non-struct key: the key column is named `value` again, not `key`.
    "spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue": "true",
    # Dataset.as[T] / Row -> case class conversion allows the pre-3.0 loose up-casts (e.g. Decimal -> Int).
    "spark.sql.legacy.doLooseUpcast": "true",
    # provider/location/owner (3.0), CREATE TABLE LIKE properties (3.2) and `external` (3.3) are reserved
    # table/database properties; true silently drops them instead of raising ParseException.
    # The same key is referenced by the 3.0, 3.1 -> 3.2 and 3.2 -> 3.3 guide sections.
    "spark.sql.legacy.notReserveProperties": "true",
    # ADD FILE with a directory raises an error again instead of adding the directory recursively.
    "spark.sql.legacy.addSingleFileInAddFile": "true",
    # hash()/xxhash64() on MapType are allowed again (results are implementation-defined).
    "spark.sql.legacy.allowHashOnMapType": "true",
    # array()/map() with no arguments are typed as StringType again instead of NullType.
    "spark.sql.legacy.createEmptyCollectionUsingStringType": "true",
    # udf(AnyRef, DataType) is allowed again. Does NOT restore the null -> default-value change for
    # primitive-typed arguments of Scala closures: that comes from Scala 2.12, not from this flag.
    "spark.sql.legacy.allowUntypedScalaUDF": "true",
    # exists() follows the 2.4 two-valued logic (null is false) instead of three-valued logic.
    "spark.sql.legacy.followThreeValuedLogicInArrayExists": "false",
    # Literals like 1E10 parse as Decimal again instead of Double.
    "spark.sql.legacy.exponentLiteralAsDecimal.enabled": "true",
    # INTERVAL '1 10:11' DAY TO SECOND style strings follow the 2.4 (lenient) parser again.
    "spark.sql.legacy.fromDayTimeString.enabled": "true",
    # Decimal types with a negative scale (e.g. 1E10 as decimal(1, -9)) are allowed again.
    "spark.sql.legacy.allowNegativeScaleOfDecimal": "true",
    # Ambiguous self-join column references silently resolve again instead of failing analysis.
    # This restores the confusing empty-result behaviour the guide describes; prefer fixing the join.
    "spark.sql.analyzer.failAmbiguousSelfJoin": "false",
    # String vs date/timestamp comparisons coerce the date to string again instead of the string to date.
    "spark.sql.legacy.typeCoercion.datetimeToString.enabled": "true",
    # CREATE TABLE AS SELECT on a Hive SerDe table writes through Hive SerDe again instead of the
    # native parquet/orc writer.
    "spark.sql.hive.convertMetastoreCtas": "false",
    # INSERT into a partitioned Hive parquet/orc table writes through Hive SerDe again.
    "spark.sql.hive.convertInsertingPartitionedTable": "false",
    # Partition values that do not match the partition column type are not validated again.
    "spark.sql.sources.validatePartitionColumns": "false",
    # JSON "" is accepted (as null) for non-string types again instead of failing the row.
    "spark.sql.legacy.json.allowEmptyString.enabled": "true",
    # New sessions inherit the SparkContext's SQL confs again (static conf, spark-defaults.conf only).
    "spark.sql.legacy.sessionInitWithConfigDefaults": "true",
    # Hive table schema inference falls back to the 2.x INFER_AND_SAVE mode again.
    "spark.sql.hive.caseSensitiveInferenceMode": "INFER_AND_SAVE",
    # Duplicate map keys keep the last value instead of raising. In 2.4 the behaviour was undefined
    # (last-wins in practice), so this is the documented mitigation rather than an exact restore.
    "spark.sql.mapKeyDedupPolicy": "LAST_WIN",
    # ------------------------------------------------------------------
    # Spark 3.1 (3.0 -> 3.1)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-30-to-31
    # std/stddev/variance etc. return NaN instead of null for a single or no row again.
    "spark.sql.legacy.statisticalAggregate": "true",
    # grouping_id() returns an int again instead of a long.
    "spark.sql.legacy.integerGroupingId": "true",
    # CAST of struct/array/map to string uses the 3.0 (non-JSON like, [a, b]) representation again.
    "spark.sql.legacy.castComplexTypesToString.enabled": "true",
    # DataFrameReader.option("path", ...) plus load(path) does not raise again.
    "spark.sql.legacy.pathOptionBehavior.enabled": "true",
    # Views are resolved with the current session's confs again instead of the captured ones.
    "spark.sql.legacy.useCurrentConfigsForView": "true",
    # Temporary views store the analyzed plan again instead of re-analyzing the SQL text.
    "spark.sql.legacy.storeAnalyzedPlanForView": "true",
    # CHAR(n)/VARCHAR(n) are treated as STRING again (no length enforcement / padding).
    "spark.sql.legacy.charVarcharAsString": "true",
    # 3.0.2 (#upgrading-from-spark-sql-301-to-302): PARTITION(col=null) parses as the string "null"
    # again for string partition columns instead of a null literal.
    "spark.sql.legacy.parseNullPartitionSpecAsStringLiteral": "true",
    # Parquet INT96 timestamps are rebased between the Julian and Proleptic Gregorian calendars again
    # (only affects files without Spark version metadata). These are the 4.0 key names; the older
    # spark.sql.legacy.parquet.int96RebaseModeIn{Read,Write} aliases were removed in 4.0.
    "spark.sql.parquet.int96RebaseModeInRead": "LEGACY",
    "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",
    # ------------------------------------------------------------------
    # Spark 3.2 (3.1 -> 3.2)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-31-to-32
    # Parquet TIMESTAMP(NANOS) columns are read as LongType again instead of failing.
    "spark.sql.legacy.parquet.nanosAsLong": "true",
    # SHOW TABLES / SHOW TBLPROPERTIES / DESCRIBE ... keep their pre-3.2 output schemas.
    "spark.sql.legacy.keepCommandOutputSchema": "true",
    # count(tblName.*) is allowed again instead of raising AnalysisException.
    "spark.sql.legacy.allowStarWithSingleTableIdentifierInCount": "true",
    # INTERVAL literals / arithmetic produce CalendarIntervalType again. Note this also disables the
    # ANSI year-month / day-time interval types entirely.
    "spark.sql.legacy.interval.enabled": "true",
    # Auto-generated column aliases (e.g. `(a + 1)`) are allowed in CREATE VIEW again.
    "spark.sql.legacy.allowAutoGeneratedAliasForView": "true",
    # ------------------------------------------------------------------
    # Spark 3.3 (3.2 -> 3.3)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-32-to-33
    # histogram_numeric returns double bins again instead of propagating the input type.
    "spark.sql.legacy.histogramNumericPropagateInputType": "true",
    # lpad/rpad on BINARY return STRING again instead of BINARY.
    "spark.sql.legacy.lpadRpadAlwaysReturnString": "true",
    # spark.read.text / Dataset[String] conversions ignore nullability again.
    "spark.sql.legacy.respectNullabilityInTextDatasetConversion": "true",
    # CSV writer emits "" (quoted empty string) for null values again instead of an empty field.
    "spark.sql.legacy.nullValueWrittenAsQuotedEmptyStringCsv": "true",
    # INSERT OVERWRITE DIRECTORY ... STORED AS parquet/orc writes through Hive SerDe again.
    "spark.sql.hive.convertMetastoreInsertDir": "false",
    # ------------------------------------------------------------------
    # Spark 3.4 (3.3 -> 3.4)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-33-to-34
    # Schema-inference changes (these alter inferred types, keep them):
    # Parquet INT64 isAdjustedToUTC=false timestamps are inferred as TIMESTAMP again, not TIMESTAMP_NTZ.
    # (The guide also lists this key under 3.2 -> 3.3 for 3.3.x builds; the conf was added in 3.4.0.)
    "spark.sql.parquet.inferTimestampNTZ.enabled": "false",
    # PySpark - https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-3-3-to-3-4
    # createDataFrame infers array element types from the first element again instead of merging.
    "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled": "true",
    # Behaviour changes (restoring these re-enables the pre-3.4 errors):
    # INSERT with a short explicit column list fails again instead of filling NULL / DEFAULT.
    "spark.sql.defaultColumn.useNullsForMissingDefaultValues": "false",
    # Three-part identifiers (spark_catalog.db.t) are rejected by v1 commands again.
    "spark.sql.legacy.v1IdentifierNoCatalog": "true",
    # ALTER TABLE ... PARTITION skips partition-spec type validation again.
    "spark.sql.legacy.skipTypeValidationOnAlterPartition": "true",
    # Performance features enabled by default in 3.4; results are unchanged, so
    # these can be dropped once the upgraded pipeline has been validated.
    "spark.sql.optimizer.runtime.bloomFilter.enabled": "false",
    "spark.sql.parquet.enableNestedColumnVectorizedReader": "false",
    "spark.sql.orc.enableNestedColumnVectorizedReader": "false",
    # ------------------------------------------------------------------
    # Spark 3.5 (3.4 -> 3.5)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-34-to-35
    # AQE / bucketed-scan rules no longer change the output partitioning of cached plans (perf only).
    "spark.sql.optimizer.canChangeCachedPlanOutputPartitioning": "false",
    # Thrift server does not interrupt the running task on cancel (thrift server only).
    "spark.sql.thriftServer.interruptOnCancel": "false",
    # array_insert negative-index semantics. Note the 1-based behaviour was also backported to
    # 3.4.2+, so this restores the pre-3.4.2 (0-based) behaviour.
    "spark.sql.legacy.negativeIndexInArrayInsert": "true",
    # Avro reads Interval as Date/Timestamp and lower-precision Decimals leniently again.
    # Key exists only from Spark 3.5.1; on 3.5.0 it is stored but has no effect (no warning is logged).
    "spark.sql.legacy.avro.allowIncompatibleSchema": "true",
    # ------------------------------------------------------------------
    # Spark 4.0 (3.5 -> 4.0)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-35-to-40
    "spark.sql.ansi.enabled": "false",
    # CREATE TABLE without USING/STORED AS creates a Hive SerDe table again instead of a
    # spark.sql.sources.default (parquet) data-source table (requires Hive support, as in 3.5).
    # Equivalent env var: SPARK_SQL_LEGACY_CREATE_HIVE_TABLE=true.
    "spark.sql.legacy.createHiveTableByDefault": "true",
    "spark.sql.maxSinglePartitionBytes": "9223372036854775807",
    "spark.sql.orc.compression.codec": "snappy",
    "spark.sql.legacy.javaCharsets": "true",
    "spark.sql.legacy.codingErrorAction": "true",
    "spark.sql.legacy.disableMapKeyNormalization": "true",
    "spark.sql.legacy.bangEqualsNot": "true",
    # Restore values for a 3.x source. From a 2.4 source BOTH must be LEGACY instead (see the
    # note above the dict).
    "spark.sql.legacy.ctePrecedencePolicy": "EXCEPTION",
    "spark.sql.legacy.timeParserPolicy": "EXCEPTION",
    "spark.sql.legacy.viewSchemaCompensation": "false",
    "spark.sql.legacy.viewSchemaBindingMode": "false",
    "spark.sql.legacy.readFileSourceTableCacheIgnoreOptions": "true",
    "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "false",
    "spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled": "true",
    # SQL - JDBC datasource type mapping changes (3.5 -> 4.0)
    "spark.sql.legacy.postgres.datetimeMapping.enabled": "true",
    "spark.sql.legacy.mysql.timestampNTZMapping.enabled": "true",
    "spark.sql.legacy.mysql.bitArrayMapping.enabled": "true",
    "spark.sql.legacy.mssqlserver.numericMapping.enabled": "true",
    "spark.sql.legacy.mssqlserver.datetimeoffsetMapping.enabled": "true",
    "spark.sql.legacy.oracle.timestampMapping.enabled": "true",
    "spark.sql.legacy.db2.numericMapping.enabled": "true",
    "spark.sql.legacy.db2.booleanMapping.enabled": "true",
    # Core - https://spark.apache.org/docs/latest/core-migration-guide.html#upgrading-from-core-35-to-40
    "spark.eventLog.rolling.enabled": "false",
    "spark.eventLog.compress": "false",
    "spark.worker.cleanup.enabled": "false",
    "spark.shuffle.service.db.backend": "LEVELDB",
    "spark.kubernetes.legacy.useReadWriteOnceAccessMode": "true",
    "spark.jars.ivy": "~/.ivy2",
    "spark.speculation.multiplier": "1.5",
    "spark.speculation.quantile": "0.75",
    "spark.log.legacyTaskNameMdc.enabled": "true",
    # Core - K8s executor pod allocation batch size. The default went 5 -> 10 in 4.0
    # and 10 -> 20 in 4.2, so restore the pre-4.0 value for a 3.x -> 4.x migration.
    "spark.kubernetes.allocation.batch.size": "5",
    # ------------------------------------------------------------------
    # Spark 4.1 (4.0 -> 4.1)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-40-to-41
    "spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing": "true",
    "spark.sql.legacy.hive.thriftServer.useZeroBasedColumnOrdinalPosition": "true",
    # PySpark (4.0 -> 4.1)
    "spark.sql.execution.pythonUDF.arrow.legacy.fallbackOnUDT": "true",
    "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": "true",
    "spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled": "true",
    "spark.sql.execution.pyspark.binaryAsBytes": "false",
    "spark.sql.execution.pandas.convertToArrowArraySafely": "false",
    # Core - https://spark.apache.org/docs/latest/core-migration-guide.html#upgrading-from-core-40-to-41
    "spark.master.rest.enabled": "false",
    "spark.checkpoint.compress": "false",
    "spark.hadoop.fs.s3a.committer.magic.enabled": "false",
    "spark.io.compression.lzf.parallel.enabled": "false",
    "spark.io.mode.default": "NIO",
    # ------------------------------------------------------------------
    # Spark 4.2 (4.1 -> 4.2) - preview, may change before the final release
    # SQL - https://spark.apache.org/docs/4.2.0-preview4/sql-migration-guide.html#upgrading-from-spark-sql-41-to-42
    "spark.sql.shuffle.orderIndependentChecksum.enabled": "false",
    "spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch": "false",
    # PySpark (4.1 -> 4.2) - Arrow becomes the default for data exchange and Python UDF/UDTF execution
    "spark.sql.execution.arrow.pyspark.enabled": "false",
    "spark.sql.execution.pythonUDF.arrow.enabled": "false",
    "spark.sql.execution.pythonUDTF.arrow.enabled": "false",
    # Core - https://spark.apache.org/docs/4.2.0-preview4/core-migration-guide.html#upgrading-from-core-41-to-42
    "spark.master.rest.virtualThread.enabled": "false",
}
