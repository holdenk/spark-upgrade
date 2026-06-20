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
# This mirrors the Scala scalafix rule RDDToDatasetMigrationCheck: it looks at
# the RDD operations used in a script and leaves a code hint about whether the
# RDD usage is simple enough to migrate to the typed DataFrame/Dataset API.
#
# Like the Scala rule it is conservative about the "can be migrated" verdict: a
# script is only advertised as migratable when *every* RDD operation it uses is
# one of the narrowly supported ones. If the script uses any RDD operation with
# no DataFrame/Dataset equivalent, only those blocking operations are flagged and
# the simple operations are left alone, so we never claim a pipeline is migratable
# when it is not.
#
# Because PySpark is dynamically typed we cannot always tell an RDD apart from a
# DataFrame, so this is a fuzzy code-hint rule. To keep the false positive rate
# low it only looks at RDD-specific method names (operations that the DataFrame
# API does not also expose, so e.g. filter/distinct/union/count/collect are
# intentionally ignored). The rule is disabled by default; enable it with a
# config override (PYRDD-DS-001: {enabled: true}).


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
        "reduceByKeyLocally": "key-based aggregation; use groupBy(...).agg(...)",
        "sampleByKey": "use DataFrame.stat.sampleBy(...)",
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
        "saveAsTextFile": "use DataFrameWriter, e.g. df.write.text(...)",
        "saveAsSequenceFile": "use DataFrameWriter",
        "saveAsPickleFile": "use DataFrameWriter, e.g. df.write.parquet(...)",
        "saveAsHadoopFile": "use DataFrameWriter",
        "saveAsNewAPIHadoopFile": "use DataFrameWriter",
        "saveAsHadoopDataset": "use DataFrameWriter",
        "saveAsNewAPIHadoopDataset": "use DataFrameWriter",
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
