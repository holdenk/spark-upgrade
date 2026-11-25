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

from pysparkler.pyspark_24_to_30 import (
    CreateDataFrameVerifySchemaCommentWriter,
    MlParamMixinsSetterCommentWriter,
    PandasConvertToArrowArraySafelyCommentWriter,
    PandasUdfUsageTransformer,
    PyArrowEnabledCommentWriter,
    RequiredPandasVersionCommentWriter,
    RowFieldNamesNotSortedCommentWriter,
    ToPandasUsageTransformer,
)
from tests.conftest import rewrite


def test_adds_required_pandas_version_comment_to_import_statements_without_alias():
    given_code = """
import pandas
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_adds_required_pandas_version_comment_to_import_statements_with_alias():
    given_code = """
import pandas as pd
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas as pd  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_adds_required_pandas_version_comment_to_from_import_statements_without_alias():
    given_code = """
from pandas import DataFrame
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
from pandas import DataFrame  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_adds_required_pandas_version_comment_to_from_import_statements_with_alias():
    given_code = """
from pandas import DataFrame as df
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
from pandas import DataFrame as df  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_required_pandas_version_comment_idempotency():
    given_code = """
import pandas as pd # PY24-30-001: An existing comment added by this transformer  # noqa: E501
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas as pd # PY24-30-001: An existing comment added by this transformer  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_does_not_overwrite_an_exising_user_comment():
    given_code = """
import pandas as pd # An existing comment
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas as pd # An existing comment  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # noqa: E501
import pyspark
"""
    assert modified_code == expected_code


def test_writes_comment_when_topandas_func_is_used_without_import():
    given_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)

pandasDF = pysparkDF.toPandas()
print(pandasDF)
"""
    modified_code = rewrite(given_code, ToPandasUsageTransformer())
    expected_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)

pandasDF = pysparkDF.toPandas()  # PY24-30-002: PySpark 3.0 requires a pandas version of 0.23.2 or higher to use toPandas()  # noqa: E501
print(pandasDF)
"""
    assert modified_code == expected_code


def test_writes_comment_when_pandas_udf_is_used_in_an_import():
    given_code = """\
import pyspark.sql.functions.pandas_udf
import pyspark.sql.functions.PandasUDFType

from pyspark.sql.types import IntegerType, StringType

str_len = pandas_udf(lambda s: s.str.len(), IntegerType())
"""
    modified_code = rewrite(given_code, PandasUdfUsageTransformer())
    expected_code = """\
import pyspark.sql.functions.pandas_udf  # PY24-30-003: PySpark 3.0 requires PyArrow version 0.12.1 or higher to use pandas_udf  # noqa: E501
import pyspark.sql.functions.PandasUDFType

from pyspark.sql.types import IntegerType, StringType

str_len = pandas_udf(lambda s: s.str.len(), IntegerType())
"""
    assert modified_code == expected_code


def test_writes_comment_when_pandas_udf_is_used_in_a_from_import():
    given_code = """\
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, StringType

str_len = pandas_udf(lambda s: s.str.len(), IntegerType())
"""
    modified_code = rewrite(given_code, PandasUdfUsageTransformer())
    expected_code = """\
from pyspark.sql.functions import pandas_udf, PandasUDFType  # PY24-30-003: PySpark 3.0 requires PyArrow version 0.12.1 or higher to use pandas_udf  # noqa: E501
from pyspark.sql.types import IntegerType, StringType

str_len = pandas_udf(lambda s: s.str.len(), IntegerType())
"""
    assert modified_code == expected_code


def test_writes_comment_when_spark_sql_execution_arrow_enabled():
    given_code = """\
import numpy as np
import pandas as pd

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
"""
    modified_code = rewrite(given_code, PyArrowEnabledCommentWriter())
    expected_code = """\
import numpy as np
import pandas as pd

spark.conf.set("spark.sql.execution.arrow.enabled", "true")  # PY24-30-004: PySpark 3.0 requires PyArrow version 0.12.1 or higher when spark.sql.execution.arrow.enabled is set to true  # noqa: E501
pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
"""
    assert modified_code == expected_code


def test_writes_comment_when_spark_sql_execution_arrow_pyspark_enabled():
    given_code = """\
import numpy as np
import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
"""
    modified_code = rewrite(given_code, PyArrowEnabledCommentWriter())
    expected_code = """\
import numpy as np
import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # PY24-30-004: PySpark 3.0 requires PyArrow version 0.12.1 or higher when spark.sql.execution.arrow.enabled is set to true  # noqa: E501
pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
"""
    assert modified_code == expected_code


def test_writes_comment_when_spark_sql_execution_arrow_enabled_in_session_builder():
    given_code = """\
import numpy as np
import pandas as pd

spark = (
    SparkSession
    .builder
    .appName("Your App Name")
    .config("spark.some.config.option1", "some-value")
    .config('spark.sql.execution.arrow.enabled', 'true')
    .getOrCreate())

pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
"""
    modified_code = rewrite(given_code, PyArrowEnabledCommentWriter())
    expected_code = """\
import numpy as np
import pandas as pd

spark = (
    SparkSession
    .builder
    .appName("Your App Name")
    .config("spark.some.config.option1", "some-value")
    .config('spark.sql.execution.arrow.enabled', 'true')
    .getOrCreate())  # PY24-30-004: PySpark 3.0 requires PyArrow version 0.12.1 or higher when spark.sql.execution.arrow.enabled is set to true  # noqa: E501

pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
"""
    assert modified_code == expected_code


def test_writes_comment_for_convert_to_arrow_array_safely_when_spark_sql_execution_arrow_enabled():
    given_code = """\
import numpy as np
import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
"""
    modified_code = rewrite(given_code, PandasConvertToArrowArraySafelyCommentWriter())
    expected_code = """\
import numpy as np
import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # PY24-30-005: Consider setting spark.sql.execution.pandas.convertToArrowArraySafely to true to raise errors in case of Integer overflow or Floating point truncation, instead of silent allows.  # noqa: E501
pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
"""
    assert modified_code == expected_code


def test_writes_comment_when_verify_schema_set_to_true_while_creating_data_frame():
    given_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns, verifySchema = True)

pandasDF = pysparkDF.toPandas()
print(pandasDF)
"""
    modified_code = rewrite(given_code, CreateDataFrameVerifySchemaCommentWriter())
    expected_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns, verifySchema = True)  # PY24-30-006: Setting verifySchema to True validates LongType as well in PySpark 3.0. Previously, LongType was not verified and resulted in None in case the value overflows.  # noqa: E501

pandasDF = pysparkDF.toPandas()
print(pandasDF)
"""
    assert modified_code == expected_code


def test_writes_comment_when_row_with_field_names_are_constructed():
    given_code = """\
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName('example').getOrCreate()

data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]

rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())
"""
    modified_code = rewrite(given_code, RowFieldNamesNotSortedCommentWriter())
    expected_code = """\
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName('example').getOrCreate()

data = [Row(lang=["Java","Scala","C++"],name="James,,Smith",state="CA"),
    Row(lang=["CSharp","VB"],name="Robert,,Williams",state="NV")]  # PY24-30-007: Sorting Row fields by name alphabetically since as of Spark 3.0, they are no longer when constructed with named arguments.  # noqa: E501

rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())
"""
    assert modified_code == expected_code


def test_does_not_write_comment_when_row_are_constructed_without_field_names_():
    given_code = """\
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName('example').getOrCreate()

data = [Row("James,,Smith",["Java","Scala","C++"],"CA"),
    Row("Robert,,Williams",["CSharp","VB"],"NV")]

rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())
"""
    modified_code = rewrite(given_code, RowFieldNamesNotSortedCommentWriter())
    assert modified_code == given_code


def test_writes_comment_when_pyspark_ml_param_shared_is_used_in_from_import():
    given_code = """\
from pyspark.ml.param.shared import *
"""
    modified_code = rewrite(given_code, MlParamMixinsSetterCommentWriter())
    expected_code = """\
from pyspark.ml.param.shared import *  # PY24-30-008: In Spark 3.0, pyspark.ml.param.shared.Has* mixins do not provide any set*(self, value) setter methods anymore, use the respective self.set(self.*, value) instead.  # noqa: E501
"""
    assert modified_code == expected_code
