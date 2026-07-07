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
    AssertPandasOnSparkEqualRemoved,
    FactorizeNaSentinelRenamed,
    PandasAppendRemoved,
    PandasBetweenTimeInclusiveRemoved,
    PandasCategoricalInplaceRemoved,
    PandasDateRangeClosedRemoved,
    PandasGetDtypeCountsRemoved,
    PandasGroupByBackfillPadRemoved,
    PandasIndexApisRemoved,
    PandasIndexClassesRemoved,
    PandasInfoNullCountsRemoved,
    PandasIsMonotonicRemoved,
    PandasIteritemsRemoved,
    PandasKoalasAccessorRemoved,
    PandasMadRemoved,
    PandasPlotSortColumnsRemoved,
    PandasReadCsvExcelParamsRemoved,
    PandasSeriesBetweenBooleanInclusiveRemoved,
    PandasToExcelParamsRemoved,
    PandasToKoalasRemoved,
    PandasToLatexColSpaceRemoved,
    PandasToSparkIoRemoved,
    PandasWeekOfYearRemoved,
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


def test_adds_ansi_hint_to_multi_name_pandas_on_spark_import():
    given_code = """
import os, pyspark.pandas as ps
"""
    modified_code = rewrite(given_code, AnsiModeEnabledByDefault())
    assert "# PY35-40-007:" in modified_code


def test_adds_ansi_hint_to_pandas_on_spark_submodule_import():
    given_code = """
from pyspark.pandas.config import option_context
"""
    modified_code = rewrite(given_code, AnsiModeEnabledByDefault())
    assert "# PY35-40-007:" in modified_code


def test_adds_ansi_hint_to_from_pyspark_import_pandas():
    given_code = """
from pyspark import pandas as ps
"""
    modified_code = rewrite(given_code, AnsiModeEnabledByDefault())
    assert "# PY35-40-007:" in modified_code


def test_does_nothing_for_plain_pandas_import():
    given_code = """
import pandas as pd
from pyspark import SparkConf
"""
    modified_code = rewrite(given_code, AnsiModeEnabledByDefault())
    assert modified_code == given_code


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


def test_adds_code_hint_when_append_is_used():
    given_code = """
combined = df.append(other)
"""
    modified_code = rewrite(given_code, PandasAppendRemoved())
    assert "# PY35-40-011:" in modified_code
    assert "ps.concat" in modified_code


def test_adds_code_hint_when_mad_is_used():
    given_code = """
result = df.mad()
"""
    modified_code = rewrite(given_code, PandasMadRemoved())
    assert "# PY35-40-012:" in modified_code


def test_adds_code_hint_when_to_spark_io_is_used():
    given_code = """
df.to_spark_io(path)
"""
    modified_code = rewrite(given_code, PandasToSparkIoRemoved())
    expected_code = """
df.to_spark_io(path)  # PY35-40-013: As of PySpark 4.0, DataFrame.to_spark_io has been removed from pandas API on Spark, use DataFrame.spark.to_spark_io instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_get_dtype_counts_is_used():
    given_code = """
counts = df.get_dtype_counts()
"""
    modified_code = rewrite(given_code, PandasGetDtypeCountsRemoved())
    expected_code = """
counts = df.get_dtype_counts()  # PY35-40-014: As of PySpark 4.0, DataFrame.get_dtype_counts has been removed from pandas API on Spark, use DataFrame.dtypes.value_counts() instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_code_hint_when_koalas_accessor_is_used():
    given_code = """
frame = df.koalas.to_frame()
"""
    modified_code = rewrite(given_code, PandasKoalasAccessorRemoved())
    assert "# PY35-40-015:" in modified_code
    assert ".pandas_on_spark" in modified_code


def test_does_nothing_for_pandas_on_spark_accessor():
    given_code = """
frame = df.pandas_on_spark.to_frame()
"""
    modified_code = rewrite(given_code, PandasKoalasAccessorRemoved())
    assert modified_code == given_code


def test_adds_code_hint_for_removed_index_apis():
    given_code = """
values = idx.asi8
compatible = idx.is_type_compatible(other)
all_dates = idx.is_all_dates
"""
    modified_code = rewrite(given_code, PandasIndexApisRemoved())
    assert modified_code.count("# PY35-40-016:") == 3


def test_adds_code_hint_when_is_monotonic_is_used():
    given_code = """
mono = series.is_monotonic
"""
    modified_code = rewrite(given_code, PandasIsMonotonicRemoved())
    expected_code = """
mono = series.is_monotonic  # PY35-40-017: As of PySpark 4.0, Series.is_monotonic and Index.is_monotonic have been removed from pandas API on Spark, use is_monotonic_increasing instead.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_for_is_monotonic_increasing():
    given_code = """
mono = series.is_monotonic_increasing
"""
    modified_code = rewrite(given_code, PandasIsMonotonicRemoved())
    assert modified_code == given_code


def test_adds_code_hint_when_weekofyear_is_used():
    given_code = """
weeks = idx.weekofyear
"""
    modified_code = rewrite(given_code, PandasWeekOfYearRemoved())
    assert "# PY35-40-018:" in modified_code


def test_does_nothing_for_isocalendar_week():
    given_code = """
weeks = series.dt.isocalendar().week
"""
    modified_code = rewrite(given_code, PandasWeekOfYearRemoved())
    assert modified_code == given_code


def test_adds_code_hint_when_groupby_backfill_or_pad_is_used():
    given_code = """
filled = df.groupby("a").backfill()
padded = df.groupby("a").pad()
"""
    modified_code = rewrite(given_code, PandasGroupByBackfillPadRemoved())
    assert modified_code.count("# PY35-40-019:") == 2


def test_adds_code_hint_when_categorical_inplace_is_used():
    given_code = """
cat.add_categories(["x"], inplace=True)
"""
    modified_code = rewrite(given_code, PandasCategoricalInplaceRemoved())
    assert "# PY35-40-020:" in modified_code


def test_does_nothing_for_categorical_without_inplace():
    given_code = """
cat = cat.add_categories(["x"])
"""
    modified_code = rewrite(given_code, PandasCategoricalInplaceRemoved())
    assert modified_code == given_code


def test_adds_code_hint_when_date_range_uses_closed():
    given_code = """
rng = ps.date_range("2021-01-01", periods=3, closed="left")
"""
    modified_code = rewrite(given_code, PandasDateRangeClosedRemoved())
    assert "# PY35-40-021:" in modified_code


def test_does_nothing_for_date_range_with_inclusive():
    given_code = """
rng = ps.date_range("2021-01-01", periods=3, inclusive="left")
"""
    modified_code = rewrite(given_code, PandasDateRangeClosedRemoved())
    assert modified_code == given_code


def test_adds_code_hint_when_between_time_uses_include_start():
    given_code = """
subset = df.between_time("0:00", "1:00", include_start=False)
"""
    modified_code = rewrite(given_code, PandasBetweenTimeInclusiveRemoved())
    assert "# PY35-40-022:" in modified_code


def test_adds_code_hint_when_between_uses_boolean_inclusive():
    given_code = """
mask = series.between(1, 5, inclusive=True)
"""
    modified_code = rewrite(given_code, PandasSeriesBetweenBooleanInclusiveRemoved())
    assert "# PY35-40-023:" in modified_code


def test_does_nothing_for_between_with_string_inclusive():
    given_code = """
mask = series.between(1, 5, inclusive="both")
"""
    modified_code = rewrite(given_code, PandasSeriesBetweenBooleanInclusiveRemoved())
    assert modified_code == given_code


def test_adds_code_hint_when_plot_uses_sort_columns():
    given_code = """
df.plot(sort_columns=True)
"""
    modified_code = rewrite(given_code, PandasPlotSortColumnsRemoved())
    assert "# PY35-40-024:" in modified_code


def test_adds_code_hint_when_to_latex_uses_col_space():
    given_code = """
latex = df.to_latex(col_space=10)
"""
    modified_code = rewrite(given_code, PandasToLatexColSpaceRemoved())
    assert "# PY35-40-025:" in modified_code


def test_adds_code_hint_when_to_excel_uses_removed_params():
    given_code = """
df.to_excel("out.xlsx", encoding="utf-8")
"""
    modified_code = rewrite(given_code, PandasToExcelParamsRemoved())
    assert "# PY35-40-026:" in modified_code


def test_adds_code_hint_when_read_csv_uses_removed_params():
    given_code = """
df = ps.read_csv("in.csv", squeeze=True)
other = ps.read_excel("in.xlsx", convert_float=True)
"""
    modified_code = rewrite(given_code, PandasReadCsvExcelParamsRemoved())
    assert modified_code.count("# PY35-40-027:") == 2


def test_adds_code_hint_when_info_uses_null_counts():
    given_code = """
df.info(null_counts=True)
"""
    modified_code = rewrite(given_code, PandasInfoNullCountsRemoved())
    assert "# PY35-40-028:" in modified_code


def test_does_nothing_for_info_with_show_counts():
    given_code = """
df.info(show_counts=True)
"""
    modified_code = rewrite(given_code, PandasInfoNullCountsRemoved())
    assert modified_code == given_code


def test_adds_code_hint_when_assert_pandas_on_spark_equal_is_used():
    given_code = """
assertPandasOnSparkEqual(actual, expected)
"""
    modified_code = rewrite(given_code, AssertPandasOnSparkEqualRemoved())
    assert "# PY35-40-029:" in modified_code
    assert "assert_frame_equal" in modified_code
