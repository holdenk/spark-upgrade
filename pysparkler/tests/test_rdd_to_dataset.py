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

from pysparkler.api import PySparkler
from pysparkler.rdd_to_dataset import RddToDatasetMigrationCommentWriter
from tests.conftest import rewrite


def test_flags_simple_rdd_operation_as_migratable():
    given_code = "doubled = rdd.map(lambda x: x * 2)\n"
    modified_code = rewrite(given_code, RddToDatasetMigrationCommentWriter())
    hint = RddToDatasetMigrationCommentWriter.SIMPLE_OPS["map"]
    expected_code = (
        "doubled = rdd.map(lambda x: x * 2)  # PYRDD-DS-001: Spark RDD operation 'map' has a direct "
        f"DataFrame/Dataset equivalent ({hint}); this RDD usage is simple enough to migrate to the "
        "DataFrame/Dataset API.  # noqa: E501\n"
    )
    assert modified_code == expected_code


def test_flags_blocking_rdd_operation_as_not_migratable():
    given_code = "reduced = pairs.reduceByKey(add)\n"
    modified_code = rewrite(given_code, RddToDatasetMigrationCommentWriter())
    reason = RddToDatasetMigrationCommentWriter.BLOCKING_OPS["reduceByKey"]
    expected_code = (
        "reduced = pairs.reduceByKey(add)  # PYRDD-DS-001: Spark RDD operation 'reduceByKey' has no direct "
        f"DataFrame/Dataset equivalent ({reason}); this RDD usage likely can't be migrated to the "
        "DataFrame/Dataset API automatically.  # noqa: E501\n"
    )
    assert modified_code == expected_code


def test_flags_an_rdd_that_cannot_be_migrated():
    # A pipeline that relies on RDD operations with no DataFrame/Dataset equivalent
    # (custom partitioning, zipWithIndex, partition-aware mapping, an RDD sink).
    given_code = """\
pairs = sc.parallelize([(1, "a"), (2, "b"), (1, "c")])
partitioned = pairs.partitionBy(4)
indexed = partitioned.zipWithIndex()
tagged = indexed.mapPartitionsWithIndex(lambda idx, it: it)
pairs.saveAsTextFile("/tmp/out")
"""
    modified_code = rewrite(given_code, RddToDatasetMigrationCommentWriter())

    # The RDD creation (parallelize) is not an operation, so it is not flagged.
    parallelize_line = next(
        line for line in modified_code.splitlines() if "parallelize" in line
    )
    assert "PYRDD-DS-001" not in parallelize_line

    # Each RDD-specific blocking operation is flagged as not migratable.
    for op in [
        "partitionBy",
        "zipWithIndex",
        "mapPartitionsWithIndex",
        "saveAsTextFile",
    ]:
        assert (
            f"Spark RDD operation '{op}' has no direct DataFrame/Dataset equivalent"
            in modified_code
        )

    assert modified_code.count("PYRDD-DS-001") == 4


def test_blocking_operation_takes_precedence_when_outermost():
    given_code = "out = rdd.map(f).reduceByKey(g)\n"
    modified_code = rewrite(given_code, RddToDatasetMigrationCommentWriter())
    assert (
        "Spark RDD operation 'reduceByKey' has no direct DataFrame/Dataset equivalent"
        in modified_code
    )
    assert "simple enough to migrate" not in modified_code
    assert modified_code.count("PYRDD-DS-001") == 1


def test_blocking_operation_takes_precedence_when_innermost():
    given_code = "out = rdd.reduceByKey(g).map(f)\n"
    modified_code = rewrite(given_code, RddToDatasetMigrationCommentWriter())
    assert (
        "Spark RDD operation 'reduceByKey' has no direct DataFrame/Dataset equivalent"
        in modified_code
    )
    assert "simple enough to migrate" not in modified_code
    assert modified_code.count("PYRDD-DS-001") == 1


def test_simple_operation_not_flagged_as_migratable_when_script_has_a_blocking_op():
    # Even though map() on its own is migratable, the script also uses reduceByKey()
    # which is not, so the whole script must not be advertised as migratable.
    given_code = """\
mapped = rdd.map(f)
reduced = rdd.reduceByKey(g)
"""
    modified_code = rewrite(given_code, RddToDatasetMigrationCommentWriter())
    assert "simple enough to migrate" not in modified_code
    assert (
        "Spark RDD operation 'reduceByKey' has no direct DataFrame/Dataset equivalent"
        in modified_code
    )
    assert modified_code.count("PYRDD-DS-001") == 1
    mapped_line = next(
        line for line in modified_code.splitlines() if line.startswith("mapped")
    )
    assert "PYRDD-DS-001" not in mapped_line


def test_simple_operations_flagged_as_migratable_when_script_only_uses_supported_ops():
    given_code = """\
mapped = rdd.map(f)
flattened = rdd.flatMap(g)
"""
    modified_code = rewrite(given_code, RddToDatasetMigrationCommentWriter())
    assert modified_code.count("PYRDD-DS-001") == 2
    assert modified_code.count("simple enough to migrate") == 2


def test_does_not_flag_dataframe_operations():
    given_code = """\
df2 = df.select("a").filter(df.a > 1).distinct()
df3 = df.union(other).groupBy("a").count()
rows = df.collect()
"""
    modified_code = rewrite(given_code, RddToDatasetMigrationCommentWriter())
    assert modified_code == given_code
    assert "PYRDD-DS-001" not in modified_code


def test_rdd_migration_comment_idempotency():
    given_code = "reduced = rdd.reduceByKey(add)\n"
    once = rewrite(given_code, RddToDatasetMigrationCommentWriter())
    twice = rewrite(once, RddToDatasetMigrationCommentWriter())
    assert once == twice
    assert twice.count("PYRDD-DS-001") == 1


def test_rdd_check_is_disabled_by_default():
    transformers = PySparkler().transformers
    assert "PYRDD-DS-001" not in [t.transformer_id for t in transformers]


def test_rdd_check_can_be_enabled_via_override():
    given_overrides = {"PYRDD-DS-001": {"enabled": True}}
    transformers = PySparkler(**given_overrides).transformers
    assert "PYRDD-DS-001" in [t.transformer_id for t in transformers]
