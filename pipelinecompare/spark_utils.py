from utils import *
from pyspark.sql import DataFrame, Row, SparkSession
import sys

if sys.version_info < (3, 9):
    sys.exit("Please use Python 3.9+")

def extract_catalog(table_name: str) -> str:
    """Extract the catalog."""
    if "." in table_name:
        return table_name.split(".")[0]
    else:
        return "spark_catalog"


def get_ancestors(spark: SparkSession, table_name: str, snapshot: str) -> list[Row]:
    """Get the ancestors of a given table at a given snapshot."""
    catalog_name = extract_catalog(table_name)
    return spark.sql(
        f"""CALL {catalog_name}.system.ancestors_of(
        snapshot_id => {snapshot}, table => '{table_name}')""").collect()


def create_changelog_view(spark: SparkSession, table_name: str, start_snapshot: str, end_snapshot: str, view_name: str) -> DataFrame:
    """Create a changelog view for the provided table."""
    catalog_name = extract_catalog(table_name)
    return spark.sql(
        f"""CALL {catalog_name}.system.create_changelog_view(
        table => '{table_name}',
        options => map('start-snapshot-id','{start_snapshot}','end-snapshot-id', '{end_snapshot}'),
        changelog_view => '{view_name}'
        )""")


def drop_iceberg_internal_columns(df: DataFrame) -> DataFrame:
    """Drop the iceberg internal columns from a changelog view that would make comparisons tricky."""
    new_df = df
    # We don't drop "_change_type" because if one version inserts and the other deletes that's a diff we want to catch.
    # However change_orgidinal and _commit_snapshot_id are expected to differ even with identical end table states.
    internal = set("_change_ordinal", "_commit_snapshot_id")
    for c in df.columns:
        name = c.split("#")
        if name in iternal:
            new_df = new_df.drop(c)
    return new_df


def get_cdc_views(spark: SparkSession, ctrl_name: str, target_name: str) -> tuple[DataFrame, DataFrame]:
    """Get the changelog/CDC views of two tables with a common ancestor."""
    (ctrl_name, c_snapshot) = ctrl_name.split("@")
    (target_name, t_snapshot) = target_name.split("@")
    if ctrl_name != target_name:
        error(f"{ctrl_name} and {target_name} are not the same table.")
    # Now we need to get the table history and make sure that the table history intersects.
    ancestors_c = get_ancestors(spark, ctrl_name, c_snapshot)
    ancestors_t = get_ancestors(spark, target_name, t_snapshot)
    control_ancestor_set = set(ancestors_c)
    shared_ancestor = None
    for t in reversed(ancestors_t):
        if t in control_ancestor_set:
            shared_ancestor = t
            break
    if shared_ancestor is None:
        error(f"No shared ancestor between tables c:{ancestors_c} t:{ancestors_t}")
    try:
        c_diff_view_name = create_changelog_view(spark, ctrl_name, t.snapshot_id, c_snapshot, "c")
        t_diff_view_name = create_changelog_view(spark, ctrl_name, t.snapshot_id, t_snapshot, "t")
        c_diff_view = drop_iceberg_internal_columns(spark.sql("SELECT * FROM c"))
        t_diff_view = drop_iceberg_internal_columns(spark.sql("SELECT * FROM t"))
    except Exception as e:
        error(f"Iceberg may not support change log view, doing legacy compare {e}")
    return (c_diff_view, t_diff_view)
