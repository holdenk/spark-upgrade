legacy_apped_rules = {
    # SQL - https://spark.apache.org/docs/3.0.0/sql-migration-guide.html
    "spark.sql.storeAssignmentPolicy": "Legacy",
    "spark.sql.legacy.setCommandRejectsSparkCoreConfs": "false",
    # ------------------------------------------------------------------
    # Spark 4.0 (3.5 -> 4.0)
    # SQL - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-35-to-40
    "spark.sql.ansi.enabled": "false",
    "spark.sql.maxSinglePartitionBytes": "9223372036854775807",
    "spark.sql.orc.compression.codec": "snappy",
    "spark.sql.legacy.javaCharsets": "true",
    "spark.sql.legacy.codingErrorAction": "true",
    "spark.sql.legacy.disableMapKeyNormalization": "true",
    "spark.sql.legacy.bangEqualsNot": "true",
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
