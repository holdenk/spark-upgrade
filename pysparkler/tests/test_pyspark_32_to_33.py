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

from pysparkler.pyspark_32_to_33 import (
    DataframeDropAxisIndexByDefaultCommentWriter,
    RequiredPandasVersionCommentWriter,
    SQLDataTypesReprReturnsObjectCommentWriter,
)
from tests.conftest import rewrite


def test_preserves_drop_by_column_behavior_when_axis_not_specified_without_labels_keyword():
    given_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
df.drop(['B', 'C'])
display(df)
"""
    modified_code = rewrite(given_code, DataframeDropAxisIndexByDefaultCommentWriter())
    expected_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
df.drop(['B', 'C'], axis = 1)  # PY32-33-001: As of PySpark 3.3, the drop method of pandas API on Spark DataFrame supports dropping rows by index, and sets dropping by index instead of column by default.  # noqa: E501
display(df)
"""
    assert modified_code == expected_code


def test_preserves_drop_by_column_behavior_when_axis_not_specified_with_labels_keyword():
    given_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
df.drop(labels=['B', 'C']).withColumnRenamed('A', 'B')
"""
    modified_code = rewrite(given_code, DataframeDropAxisIndexByDefaultCommentWriter())
    expected_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
df.drop(labels=['B', 'C'], axis = 1).withColumnRenamed('A', 'B')  # PY32-33-001: As of PySpark 3.3, the drop method of pandas API on Spark DataFrame supports dropping rows by index, and sets dropping by index instead of column by default.  # noqa: E501
"""
    assert modified_code == expected_code


def test_does_nothing_when_drop_by_column_with_axis_one_specified():
    given_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
df.drop(['B', 'C'], axis=1)
"""
    modified_code = rewrite(given_code, DataframeDropAxisIndexByDefaultCommentWriter())
    assert modified_code == given_code


def test_does_nothing_when_drop_by_column_with_axis_zero_specified():
    given_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
df.drop(['B', 'C'], axis=0)
"""
    modified_code = rewrite(given_code, DataframeDropAxisIndexByDefaultCommentWriter())
    assert modified_code == given_code


def test_does_nothing_when_drop_by_columns_keyword():
    given_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
df.drop(columns=['B', 'C'])
"""
    modified_code = rewrite(given_code, DataframeDropAxisIndexByDefaultCommentWriter())
    assert modified_code == given_code


def test_does_nothing_when_drop_by_index_keyword():
    given_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
df.drop(index=[0, 1], columns='A')
"""
    modified_code = rewrite(given_code, DataframeDropAxisIndexByDefaultCommentWriter())
    assert modified_code == given_code


def test_adds_required_pandas_version_comment_to_import_statements():
    given_code = """
import pandas
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_adds_comment_when_repr_is_called_on_sql_data_types():
    given_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
a_column_values = list(df['A'].unique())
repr_a_column_values = [repr(value) for value in a_column_values]
"""
    modified_code = rewrite(given_code, SQLDataTypesReprReturnsObjectCommentWriter())
    expected_code = """
import pyspark.pandas as ps

df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
a_column_values = list(df['A'].unique())
repr_a_column_values = [repr(value) for value in a_column_values]  # PY32-33-003: As of PySpark 3.3, the repr return values of SQL DataTypes have been changed to yield an object with the same value when passed to eval.  # noqa: E501
"""
    assert modified_code == expected_code
