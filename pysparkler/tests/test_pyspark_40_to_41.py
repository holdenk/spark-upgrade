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

from pysparkler.pyspark_40_to_41 import (
    BinaryTypeMapsToBytes,
    ConvertToArrowArraySafelyEnabledByDefault,
    Python39SupportDropped,
    RequiredPandasVersionCommentWriter,
    RequiredPyArrowVersionCommentWriter,
)
from tests.conftest import rewrite


def test_adds_required_pyarrow_version_comment_to_pandas_udf_import():
    given_code = """
from pyspark.sql.functions import pandas_udf
"""
    modified_code = rewrite(given_code, RequiredPyArrowVersionCommentWriter())
    expected_code = """
from pyspark.sql.functions import pandas_udf  # PY40-41-001: PySpark 4.1 requires PyArrow version 15.0.0 or higher to use pandas_udf  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_required_pandas_version_comment_to_import_statements():
    given_code = """
import pandas as pd
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas as pd  # PY40-41-002: PySpark 4.1 requires pandas version 2.2.0 or higher  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_top_level_pyspark_is_imported():
    given_code = """
import pyspark
"""
    modified_code = rewrite(given_code, Python39SupportDropped())
    expected_code = """
import pyspark  # PY40-41-003: As of PySpark 4.1, Python 3.9 support has been dropped, Python 3.10 or higher is required.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_for_pyspark_submodule_imports():
    given_code = """
import pyspark.pandas as ps
"""
    modified_code = rewrite(given_code, Python39SupportDropped())
    assert modified_code == given_code


def test_adds_code_hint_when_binary_type_is_constructed():
    given_code = """
field = StructField("bin", BinaryType())
typed = types.BinaryType()
"""
    modified_code = rewrite(given_code, BinaryTypeMapsToBytes())
    expected_code = """
field = StructField("bin", BinaryType())  # PY40-41-004: As of PySpark 4.1, BinaryType is mapped to Python bytes instead of bytearray. To restore the previous behavior, set spark.sql.execution.pyspark.binaryAsBytes to false.  # noqa: E501
typed = types.BinaryType()  # PY40-41-004: As of PySpark 4.1, BinaryType is mapped to Python bytes instead of bytearray. To restore the previous behavior, set spark.sql.execution.pyspark.binaryAsBytes to false.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_pandas_udf_is_used():
    given_code = """
my_udf = pandas_udf(my_func, returnType=LongType())
"""
    modified_code = rewrite(given_code, ConvertToArrowArraySafelyEnabledByDefault())
    expected_code = """
my_udf = pandas_udf(my_func, returnType=LongType())  # PY40-41-005: As of PySpark 4.1, spark.sql.execution.pandas.convertToArrowArraySafely is enabled by default, so PyArrow raises errors on unsafe conversions (integer overflow, float truncation, loss of precision). To restore the previous behavior, set it to false.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_top_level_pyspark_imported_alongside_other_modules():
    given_code = """
import os, pyspark
"""
    modified_code = rewrite(given_code, Python39SupportDropped())
    expected_code = """
import os, pyspark  # PY40-41-003: As of PySpark 4.1, Python 3.9 support has been dropped, Python 3.10 or higher is required.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_when_pyspark_not_imported_at_top_level():
    given_code = """
import os
import sys
"""
    modified_code = rewrite(given_code, Python39SupportDropped())
    assert modified_code == given_code


def test_does_nothing_when_binary_type_absent():
    given_code = """
field = StructField("name", StringType())
"""
    modified_code = rewrite(given_code, BinaryTypeMapsToBytes())
    assert modified_code == given_code


def test_adds_code_hint_when_pandas_udf_used_as_module_attribute():
    given_code = """
my_udf = F.pandas_udf(my_func, returnType=LongType())
"""
    modified_code = rewrite(given_code, ConvertToArrowArraySafelyEnabledByDefault())
    assert "# PY40-41-005:" in modified_code and modified_code != given_code


def test_does_nothing_for_convert_safely_when_pandas_udf_absent():
    given_code = """
result = df.select("a")
"""
    modified_code = rewrite(given_code, ConvertToArrowArraySafelyEnabledByDefault())
    assert modified_code == given_code
