def load_spark_config(file: str) -> dict:


legacy_apped_rules = {
    # SQL - https://spark.apache.org/docs/3.0.0/sql-migration-guide.html
    "spark.sql.storeAssignmentPolicy": "Legacy"
    "spark.sql.legacy.setCommandRejectsSparkCoreConfs": "false"

    }
