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

from pysparkler.pyspark_41_to_42 import (
    ArrowDataExchangeEnabledByDefault,
    ArrowOptimizedPythonUdfByDefault,
    ArrowOptimizedPythonUdtfByDefault,
    RequiredPyArrowVersionCommentWriter,
)
from tests.conftest import rewrite


def test_adds_required_pyarrow_version_comment_to_pandas_udf_import():
    given_code = """
from pyspark.sql.functions import pandas_udf
"""
    modified_code = rewrite(given_code, RequiredPyArrowVersionCommentWriter())
    expected_code = """
from pyspark.sql.functions import pandas_udf  # PY41-42-001: PySpark 4.2 requires PyArrow version 18.0.0 or higher to use pandas_udf  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_to_pandas_is_used():
    given_code = """
pdf = df.toPandas()
"""
    modified_code = rewrite(given_code, ArrowDataExchangeEnabledByDefault())
    expected_code = """
pdf = df.toPandas()  # PY41-42-002: As of PySpark 4.2, columnar data exchange between PySpark and the JVM uses Apache Arrow by default (spark.sql.execution.arrow.pyspark.enabled defaults to true). To restore the legacy row-based exchange, set it to false.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_python_udf_is_created():
    given_code = """
greet = udf(my_func, StringType())
"""
    modified_code = rewrite(given_code, ArrowOptimizedPythonUdfByDefault())
    expected_code = """
greet = udf(my_func, StringType())  # PY41-42-003: As of PySpark 4.2, regular Python UDFs are Arrow-optimized by default (spark.sql.execution.pythonUDF.arrow.enabled defaults to true). To restore the legacy behavior, set it to false.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_python_udtf_is_created():
    given_code = """
my_table_func = udtf(MyUdtf, returnType="x: int")
"""
    modified_code = rewrite(given_code, ArrowOptimizedPythonUdtfByDefault())
    expected_code = """
my_table_func = udtf(MyUdtf, returnType="x: int")  # PY41-42-004: As of PySpark 4.2, regular Python UDTFs are Arrow-optimized by default (spark.sql.execution.pythonUDTF.arrow.enabled defaults to true). To restore the legacy behavior, set it to false.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_when_no_udf_or_to_pandas_present():
    given_code = """
result = df.select("a", "b")
"""
    assert rewrite(given_code, ArrowOptimizedPythonUdfByDefault()) == given_code
    assert rewrite(given_code, ArrowDataExchangeEnabledByDefault()) == given_code
