#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#  #
#    http://www.apache.org/licenses/LICENSE-2.0
#  #
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#

import libcst as cst
import libcst.matchers as m

from pysparkler.base import StatementLineCommentWriter

# A simplistic (and optional) RDD -> Dataset/DataFrame migration check.
#
# This loosely mirrors the Scala scalafix rule RDDToDatasetMigrationCheck: it
# looks at the RDD operations used in a script and leaves a code hint about
# whether the RDD usage looks simple enough to migrate to the typed
# DataFrame/Dataset API.
#
# IMPORTANT: unlike the Scala rule, this is a *best-effort, name-based* heuristic.
# The Scala rule resolves method-owner symbols and is safe-by-default (any RDD op
# it does not recognise is treated as blocking), so it cannot over-claim. Here we
# only see method *names* (PySpark is dynamically typed, so we cannot tell an RDD
# apart from a DataFrame, a dict, or a list), so:
#   - The BLOCKING_OPS list below covers many common RDD-only operations, but it
#     cannot be exhaustive -- an RDD-only op we did not enumerate will not trip
#     the gate. The "can be migrated" hint is therefore ADVISORY: it means "the
#     RDD ops seen here have equivalents", not "the whole pipeline is provably
#     migratable". Verify the full pipeline before relying on it.
#   - To keep the false-positive rate tolerable we deliberately do NOT flag
#     operations whose names collide with very common non-Spark methods (e.g.
#     keys()/values()/max()/min()/sum() on dicts, lists, pandas), even though
#     they exist on RDDs. That is a knowing trade-off: fewer false alarms, at the
#     cost of missing those as blockers.
#
# To keep the false positive rate low it only looks at RDD-specific method names
# (operations that the DataFrame API does not also expose, so e.g.
# filter/distinct/union/count/collect are intentionally ignored). The rule is
# disabled by default; enable it with a config override (PYRDD-DS-001: {enabled: true}).


class RddToDatasetMigrationCommentWriter(StatementLineCommentWriter):
    """Flags RDD operations and hints whether the RDD usage can be migrated to the DataFrame/Dataset API.

    The "can be migrated" hint is only added when the whole script uses only the narrowly supported RDD
    operations (those with a reasonably direct DataFrame/Dataset equivalent). If any RDD-specific operation
    with no straightforward equivalent (key/pair functions, joins, zips, custom partitioning, manual
    aggregations, RDD sinks, ...) is used, only those blocking operations are flagged as not migratable.
    """

    # RDD operations that have a reasonably direct DataFrame/Dataset equivalent.
    SIMPLE_OPS = {
        "map": "use select()/withColumn() with column expressions",
        "flatMap": "use select() with explode()",
        "reduce": "use an aggregation such as agg()",
        "sortBy": "use orderBy()/sort() with column expressions",
    }

    # RDD-specific operations that have no straightforward DataFrame/Dataset equivalent.
    BLOCKING_OPS = {
        "reduceByKey": "key-based aggregation; use groupBy(...).agg(...)",
        "groupByKey": "key-based aggregation; use groupBy(...).agg(...)",
        "aggregateByKey": "key-based aggregation; use groupBy(...).agg(...)",
        "combineByKey": "key-based aggregation; use groupBy(...).agg(...)",
        "foldByKey": "key-based aggregation; use groupBy(...).agg(...)",
        "countByKey": "use groupBy(...).count()",
        "countByValue": "use groupBy(...).count()",
        "countApprox": "approximate actions have no DataFrame equivalent",
        "countApproxDistinct": "use approx_count_distinct()",
        "sumApprox": "approximate actions have no DataFrame equivalent",
        "meanApprox": "approximate actions have no DataFrame equivalent",
        "reduceByKeyLocally": "key-based aggregation; use groupBy(...).agg(...)",
        "sampleByKey": "use DataFrame.stat.sampleBy(...)",
        "subtractByKey": "pair-RDD set op; use a left_anti join on the key column",
        "collectAsMap": "collect() the DataFrame and build the dict in Python",
        "leftOuterJoin": 'extract the join keys and use DataFrame.join(..., how="left")',
        "rightOuterJoin": 'extract the join keys and use DataFrame.join(..., how="right")',
        "fullOuterJoin": 'extract the join keys and use DataFrame.join(..., how="outer")',
        "cogroup": "no direct DataFrame equivalent; restructure with joins or groupBy",
        "groupWith": "no direct DataFrame equivalent",
        "keyBy": "derive the key as a column instead",
        "partitionBy": "custom partitioning has no DataFrame equivalent",
        "repartitionAndSortWithinPartitions": "no direct DataFrame equivalent",
        "mapPartitions": "use mapInPandas()/mapInArrow() or vectorized UDFs",
        "mapPartitionsWithIndex": "the partition index is not exposed on DataFrame",
        "mapPartitionsWithSplit": "the partition index is not exposed on DataFrame",
        "zip": "RDD.zip has no DataFrame equivalent",
        "zipWithIndex": "use monotonically_increasing_id() or a window function",
        "zipWithUniqueId": "use monotonically_increasing_id()",
        "aggregate": "manual aggregation; express it with agg()",
        "treeAggregate": "manual aggregation; express it with agg()",
        "treeReduce": "manual aggregation; express it with agg()",
        "fold": "manual aggregation; express it with agg() or reduce()",
        "glom": "no DataFrame equivalent",
        "pipe": "no DataFrame equivalent",
        "cartesian": "use crossJoin()",
        "sortByKey": "use orderBy() on the key column",
        "mapValues": "operate on the value column instead",
        "flatMapValues": "operate on the value column with explode()",
        "lookup": "use a filter on the key column",
        "takeOrdered": "use orderBy(...).limit(n)",
        "top": "use orderBy(...).limit(n)",
        "takeSample": "use sample(...).limit(n)",
        "saveAsTextFile": "use DataFrameWriter, e.g. df.write.text(...)",
        "saveAsSequenceFile": "use DataFrameWriter",
        "saveAsPickleFile": "use DataFrameWriter, e.g. df.write.parquet(...)",
        "saveAsHadoopFile": "use DataFrameWriter",
        "saveAsNewAPIHadoopFile": "use DataFrameWriter",
        "saveAsHadoopDataset": "use DataFrameWriter",
        "saveAsNewAPIHadoopDataset": "use DataFrameWriter",
        # DoubleRDD statistics ops (Spark-specific names, low collision risk).
        "histogram": "no DataFrame equivalent; bucket with a window/agg expression",
        "stats": "use describe()/summary() or agg() with the stat functions",
        "stdev": "use agg(stddev_pop(...))",
        "sampleStdev": "use agg(stddev_samp(...))",
        "variance": "use agg(var_pop(...))",
        "sampleVariance": "use agg(var_samp(...))",
        # Other RDD-only ops with Spark-specific names.
        "countByValueApprox": "approximate actions have no DataFrame equivalent",
        "getCheckpointFile": "no DataFrame equivalent",
        "getNumPartitions": "use df.rdd.getNumPartitions() or spark_partition_id()",
    }

    def __init__(self, comment: str | None = None):
        super().__init__(
            transformer_id="PYRDD-DS-001",
            comment=(
                comment
                if comment is not None
                else "Review this RDD usage for migration to the DataFrame/Dataset API"
            ),
        )
        # Optional / opt-in: disabled unless explicitly enabled via config override.
        self.enabled = False
        # Whether the script uses any RDD operation with no DataFrame/Dataset equivalent.
        # Computed up front so the "can be migrated" hint is only emitted when the whole
        # script uses only the narrowly supported operations.
        self._has_blocking = False

    def visit_Module(self, node: cst.Module) -> None:
        """Pre-scan the whole module to see whether any unsupported RDD operation is used."""
        self._has_blocking = any(
            isinstance(call, cst.Call)
            and isinstance(call.func, cst.Attribute)
            and call.func.attr.value in self.BLOCKING_OPS
            for call in m.findall(node, m.Call())
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Detect RDD-specific operations and set a migration hint for the statement line."""
        func = node.func
        if not isinstance(func, cst.Attribute):
            return
        op = func.attr.value
        if op in self.BLOCKING_OPS:
            self.match_found = True
            self._comment = (
                f"Spark RDD operation '{op}' has no direct DataFrame/Dataset equivalent "
                f"({self.BLOCKING_OPS[op]}); this RDD usage likely can't be migrated to the "
                "DataFrame/Dataset API automatically."
            )
        elif op in self.SIMPLE_OPS and not self._has_blocking:
            self.match_found = True
            self._comment = (
                f"Spark RDD operation '{op}' has a direct DataFrame/Dataset equivalent "
                f"({self.SIMPLE_OPS[op]}); this RDD usage is simple enough to migrate to the "
                "DataFrame/Dataset API."
            )


def rdd_to_dataset_transformers() -> list[cst.CSTTransformer]:
    """Return the optional, opt-in RDD -> Dataset/DataFrame migration check transformers"""
    return [
        RddToDatasetMigrationCommentWriter(),
    ]
