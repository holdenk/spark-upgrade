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

from pysparkler.pyspark_35_to_40 import (
    AnsiModeEnabledByDefault,
    FactorizeNaSentinelRenamed,
    PandasIndexClassesRemoved,
    PandasIteritemsRemoved,
    PandasToKoalasRemoved,
    Python38SupportDropped,
    RequiredNumpyVersionCommentWriter,
    RequiredPandasVersionCommentWriter,
    RequiredPyArrowVersionCommentWriter,
    SqlFunctionsStarImport,
)
from tests.conftest import rewrite


def test_adds_required_pandas_version_comment_to_import_statements():
    given_code = """
import pandas
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas  # PY35-40-001: PySpark 4.0 requires pandas version 2.0.0 or higher  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_adds_required_numpy_version_comment_to_import_statements():
    given_code = """
import numpy as np
import pyspark
"""
    modified_code = rewrite(given_code, RequiredNumpyVersionCommentWriter())
    expected_code = """
import numpy as np  # PY35-40-002: PySpark 4.0 requires numpy version 1.21 or higher  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_adds_required_pyarrow_version_comment_to_pandas_udf_import():
    given_code = """
from pyspark.sql.functions import pandas_udf
"""
    modified_code = rewrite(given_code, RequiredPyArrowVersionCommentWriter())
    expected_code = """
from pyspark.sql.functions import pandas_udf  # PY35-40-003: PySpark 4.0 requires PyArrow version 11.0.0 or higher to use pandas_udf  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_iteritems_is_used_in_a_simple_statement():
    given_code = """
items = df.iteritems()
"""
    modified_code = rewrite(given_code, PandasIteritemsRemoved())
    expected_code = """
items = df.iteritems()  # PY35-40-004: As of PySpark 4.0, DataFrame.iteritems and Series.iteritems have been removed from pandas API on Spark, use DataFrame.items and Series.items instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_to_koalas_is_used():
    given_code = """
kdf = df.to_koalas()
psdf = df.to_pandas_on_spark()
"""
    modified_code = rewrite(given_code, PandasToKoalasRemoved())
    expected_code = """
kdf = df.to_koalas()  # PY35-40-005: As of PySpark 4.0, DataFrame.to_koalas and DataFrame.to_pandas_on_spark have been removed, use DataFrame.pandas_api instead.  # noqa: E501
psdf = df.to_pandas_on_spark()  # PY35-40-005: As of PySpark 4.0, DataFrame.to_koalas and DataFrame.to_pandas_on_spark have been removed, use DataFrame.pandas_api instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_removed_index_classes_are_used():
    given_code = """
idx = ps.Int64Index([1, 2, 3])
"""
    modified_code = rewrite(given_code, PandasIndexClassesRemoved())
    expected_code = """
idx = ps.Int64Index([1, 2, 3])  # PY35-40-006: As of PySpark 4.0, Int64Index and Float64Index have been removed from pandas API on Spark, use Index directly instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_pandas_on_spark_is_imported():
    given_code = """
import pyspark.pandas as ps
"""
    modified_code = rewrite(given_code, AnsiModeEnabledByDefault())
    expected_code = """
import pyspark.pandas as ps  # PY35-40-007: As of PySpark 4.0, ANSI mode is enabled by default and pandas API on Spark raises an exception under ANSI mode. Disable it via spark.sql.ansi.enabled=false, or set the pandas-on-spark option compute.fail_on_ansi_mode=False to force it to work.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_to_sql_functions_star_import():
    given_code = """
from pyspark.sql.functions import *
"""
    modified_code = rewrite(given_code, SqlFunctionsStarImport())
    expected_code = """
from pyspark.sql.functions import *  # PY35-40-008: As of PySpark 4.0, items other than functions (e.g. DataFrame, Column, StructType) are no longer exported from `from pyspark.sql.functions import *`. Import them from their proper modules instead, e.g. `from pyspark.sql import DataFrame, Column` and `from pyspark.sql.types import StructType`.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_for_non_wildcard_sql_functions_import():
    given_code = """
from pyspark.sql.functions import col, lit
"""
    modified_code = rewrite(given_code, SqlFunctionsStarImport())
    assert modified_code == given_code


def test_adds_code_hint_when_factorize_uses_na_sentinel():
    given_code = """
codes, uniques = series.factorize(na_sentinel=-1)
"""
    modified_code = rewrite(given_code, FactorizeNaSentinelRenamed())
    expected_code = """
codes, uniques = series.factorize(na_sentinel=-1)  # PY35-40-009: As of PySpark 4.0, the na_sentinel parameter of factorize has been removed from pandas API on Spark, use use_na_sentinel instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_when_factorize_uses_use_na_sentinel():
    given_code = """
codes, uniques = series.factorize(use_na_sentinel=True)
"""
    modified_code = rewrite(given_code, FactorizeNaSentinelRenamed())
    assert modified_code == given_code


def test_adds_code_hint_when_index_class_is_constructed_without_module_prefix():
    given_code = """
idx = Int64Index([1, 2, 3])
"""
    modified_code = rewrite(given_code, PandasIndexClassesRemoved())
    expected_code = """
idx = Int64Index([1, 2, 3])  # PY35-40-006: As of PySpark 4.0, Int64Index and Float64Index have been removed from pandas API on Spark, use Index directly instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_for_index_class_names_that_are_not_constructed():
    # Assignment targets and keyword-argument names should not be flagged.
    given_code = """
Int64Index = 5
result = compute(Float64Index=3)
"""
    modified_code = rewrite(given_code, PandasIndexClassesRemoved())
    assert modified_code == given_code


def test_adds_python38_drop_hint_to_first_pyspark_import():
    given_code = """
import pyspark
from pyspark.sql import SparkSession
"""
    modified_code = rewrite(given_code, Python38SupportDropped())
    expected_code = """
import pyspark  # PY35-40-010: As of PySpark 4.0, Python 3.8 support has been dropped, Python 3.9 or higher is required.  # noqa: E501
from pyspark.sql import SparkSession
"""
    assert modified_code == expected_code


def test_python38_drop_hint_handles_from_pyspark_import():
    given_code = """
from pyspark.sql import SparkSession
"""
    modified_code = rewrite(given_code, Python38SupportDropped())
    expected_code = """
from pyspark.sql import SparkSession  # PY35-40-010: As of PySpark 4.0, Python 3.8 support has been dropped, Python 3.9 or higher is required.  # noqa: E501
"""
    assert modified_code == expected_code


def test_python38_drop_hint_does_nothing_without_pyspark_import():
    given_code = """
import os
import numpy as np
"""
    modified_code = rewrite(given_code, Python38SupportDropped())
    assert modified_code == given_code
