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

from pysparkler.pyspark_31_to_32 import SqlMlMethodsRaiseTypeErrorCommentWriter
from tests.conftest import rewrite


def test_adds_may_raise_type_error_when_catching_value_errors_on_sql_or_ml_from_imports():
    given_code = """
from pyspark.sql.column import Column, _to_seq, _to_list, _to_java_column
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.streaming import DataStreamWriter


def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    modified_code = rewrite(given_code, SqlMlMethodsRaiseTypeErrorCommentWriter())
    expected_code = """
from pyspark.sql.column import Column, _to_seq, _to_list, _to_java_column
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.streaming import DataStreamWriter


def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.  # noqa: E501

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    assert modified_code == expected_code


def test_adds_may_raise_type_error_with_alias_when_catching_value_errors_on_sql_or_ml_from_imports():
    given_code = """
from pyspark.ml.streaming import DataStreamWriter

def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError as ex:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    modified_code = rewrite(given_code, SqlMlMethodsRaiseTypeErrorCommentWriter())
    expected_code = """
from pyspark.ml.streaming import DataStreamWriter

def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError as ex:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.  # noqa: E501

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    assert modified_code == expected_code


def test_adds_may_raise_type_error_with_alias_when_catching_value_errors_on_sql_or_ml_import():
    given_code = """
import pyspark.sql.functions as f

def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError as ex:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    modified_code = rewrite(given_code, SqlMlMethodsRaiseTypeErrorCommentWriter())
    expected_code = """
import pyspark.sql.functions as f

def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError as ex:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.  # noqa: E501

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    assert modified_code == expected_code
