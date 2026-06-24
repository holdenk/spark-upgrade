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

from pysparkler.pyspark_common import (
    BuiltinFunctionShadowing,
    RemovedOrRenamedConfig,
    TriggerOnceDeprecated,
)
from tests.conftest import rewrite


def test_adds_hint_for_trigger_once():
    given_code = """
query = df.writeStream.trigger(once=True).start()
"""
    modified_code = rewrite(given_code, TriggerOnceDeprecated())
    expected_code = """
query = df.writeStream.trigger(once=True).start()  # PYC-001: trigger(once=True) (Trigger.Once) is deprecated since Spark 3.3. Use trigger(availableNow=True) (Trigger.AvailableNow) instead, which honors rate limits across multiple micro-batches.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_for_trigger_available_now_or_processing_time():
    given_code = """
a = df.writeStream.trigger(availableNow=True).start()
b = df.writeStream.trigger(processingTime="1 minute").start()
"""
    modified_code = rewrite(given_code, TriggerOnceDeprecated())
    assert modified_code == given_code


def test_adds_hint_for_builtin_shadowing_explicit_import():
    given_code = """
from pyspark.sql.functions import max, col
"""
    modified_code = rewrite(given_code, BuiltinFunctionShadowing())
    expected_code = """
from pyspark.sql.functions import max, col  # PYC-002: This import shadows a Python builtin of the same name from pyspark.sql.functions. Calling it with builtin-only arguments (e.g. key=) will fail. Prefer importing the module, e.g. `from pyspark.sql import functions as F`, or alias the function on import.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_hint_for_builtin_shadowing_wildcard_import():
    given_code = """
from pyspark.sql.functions import *
"""
    modified_code = rewrite(given_code, BuiltinFunctionShadowing())
    assert "# PYC-002:" in modified_code and modified_code != given_code


def test_does_nothing_for_non_shadowing_functions_import():
    given_code = """
from pyspark.sql.functions import col, lit
"""
    modified_code = rewrite(given_code, BuiltinFunctionShadowing())
    assert modified_code == given_code


def test_does_nothing_for_aliased_shadowing_import():
    given_code = """
from pyspark.sql.functions import max as spark_max
"""
    modified_code = rewrite(given_code, BuiltinFunctionShadowing())
    assert modified_code == given_code


def test_does_nothing_when_functions_module_imported_whole():
    given_code = """
from pyspark.sql import functions as F
"""
    modified_code = rewrite(given_code, BuiltinFunctionShadowing())
    assert modified_code == given_code


def test_adds_hint_and_replacement_for_renamed_config():
    given_code = """
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
"""
    modified_code = rewrite(given_code, RemovedOrRenamedConfig())
    expected_code = """
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")  # PYC-003: spark.sql.legacy.parquet.int96RebaseModeInWrite was renamed in Spark 4.x; it no longer takes effect. Use spark.sql.parquet.int96RebaseModeInWrite instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_hint_for_renamed_config_via_builder_config():
    given_code = """
spark = SparkSession.builder.config("spark.shuffle.unsafe.file.output.buffer", "32k").getOrCreate()
"""
    modified_code = rewrite(given_code, RemovedOrRenamedConfig())
    assert (
        "spark.shuffle.localDisk.file.output.buffer" in modified_code
        and "# PYC-003:" in modified_code
    )


def test_adds_hint_for_blacklist_config():
    given_code = """
spark.conf.set("spark.blacklist.enabled", "true")
"""
    modified_code = rewrite(given_code, RemovedOrRenamedConfig())
    assert "# PYC-003:" in modified_code and "excludeOnFailure" in modified_code


def test_does_nothing_for_supported_config():
    given_code = """
spark.conf.set("spark.sql.shuffle.partitions", "200")
"""
    modified_code = rewrite(given_code, RemovedOrRenamedConfig())
    assert modified_code == given_code
