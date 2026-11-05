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
    DateTimePatternLettersChanged,
    MlParamMixinsSetterCommentWriter,
    OneHotEncoderEstimatorRenamed,
    OneHotEncoderTransformWithoutFit,
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


def test_adds_hint_for_week_based_year_pattern_in_date_format():
    given_code = """
import pyspark.sql.functions as F
df = df.withColumn("d", F.date_format("ts", "YYYY-MM-dd"))
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    expected_code = """
import pyspark.sql.functions as F
df = df.withColumn("d", F.date_format("ts", "YYYY-MM-dd"))  # PY24-30-009: Datetime pattern 'YYYY-MM-dd' (letter(s) Y) uses letters whose meaning changed in Spark 3.0 (DateTimeFormatter replaced SimpleDateFormat): Y/W/w/u/e/c (week-based year, week-of-month, week-of-year, day-number-of-week, localized day-of-week) are rejected as week-based patterns and F now means aligned day-of-week-in-month (was week-of-month). Use y for the year, E for the day name or weekofyear()/dayofweek() for week computations, see https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html. To keep the 2.4 behaviour set spark.sql.legacy.timeParserPolicy=LEGACY.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_hint_for_week_based_letters_in_all_datetime_functions():
    given_code = """
import pyspark.sql.functions as F
from pyspark.sql.functions import to_date, from_unixtime
a = to_date(df.ts, format="dd/MM/yyyy ww")
b = F.to_timestamp("ts", "yyyy-MM-dd u")
c = F.unix_timestamp(df.ts, "yyyy-MM-dd e")
d = from_unixtime(df.ts, "yyyy-MM-dd c")
e = F.date_format(df.ts, "W F")
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    assert modified_code.count("# PY24-30-009:") == 5
    assert "'dd/MM/yyyy ww' (letter(s) w) uses letters" in modified_code
    assert "'yyyy-MM-dd u' (letter(s) u) uses letters" in modified_code
    assert "'yyyy-MM-dd e' (letter(s) e) uses letters" in modified_code
    assert "'yyyy-MM-dd c' (letter(s) c) uses letters" in modified_code
    assert "'W F' (letter(s) W, F) uses letters" in modified_code


def test_adds_hint_for_date_format_and_timestamp_format_options():
    given_code = """
r = spark.read.option("header", "true").option("dateFormat", "YYYY-MM-dd").csv("x")
w = spark.read.option("timestampFormat", 'yyyy-MM-dd HH:mm:ss.SSS u').csv("x")
k = spark.read.options(dateFormat="MM/dd/YYYY").csv("x")
j = spark.read.csv("x", timestampFormat="yyyy-ww")
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    assert modified_code.count("# PY24-30-009:") == 4
    assert "'yyyy-MM-dd HH:mm:ss.SSS u' (letter(s) u)" in modified_code
    assert "'MM/dd/YYYY' (letter(s) Y)" in modified_code
    assert "'yyyy-ww' (letter(s) w)" in modified_code


def test_does_nothing_for_valid_quoted_strftime_or_non_literal_datetime_patterns():
    given_code = """
import pyspark.sql.functions as F
a = F.date_format("ts", 'yyyy-MM-dd HH:mm:ss')
b = F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss")
c = F.date_format("ts", "'Week' yyyy")
d = F.date_format("ts", "'Week''s' yyyy")
e = pd.to_datetime(s, format='%Y-%m-%d')
f = F.date_format("ts", '%Y-%m-%d')
g = F.date_format("ts", fmt)
h = F.date_format("ts", f"{year}-MM-dd")
i = F.to_date(df.ts)
r = spark.read.option("header", "true").csv("x")
s = spark.read.option("dateFormat", "yyyy-MM-dd").csv("x")
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    assert modified_code == given_code


def test_renames_one_hot_encoder_estimator_from_import_and_call():
    given_code = """
from pyspark.ml.feature import OneHotEncoderEstimator
enc = OneHotEncoderEstimator(inputCols=["a"], outputCols=["b"])
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    expected_code = """
from pyspark.ml.feature import OneHotEncoder  # PY24-30-010: As of Spark 3.0, OneHotEncoderEstimator is renamed to OneHotEncoder; the old OneHotEncoder Transformer was removed.  # noqa: E501
enc = OneHotEncoder(inputCols=["a"], outputCols=["b"])  # PY24-30-010: As of Spark 3.0, OneHotEncoderEstimator is renamed to OneHotEncoder; the old OneHotEncoder Transformer was removed.  # noqa: E501
"""
    assert modified_code == expected_code


def test_renames_one_hot_encoder_estimator_aliased_import():
    given_code = """
from pyspark.ml.feature import OneHotEncoderEstimator as OHE
enc = OHE(inputCols=["a"], outputCols=["b"])
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    assert (
        "from pyspark.ml.feature import OneHotEncoder as OHE  # PY24-30-010:"
        in modified_code
    )
    assert 'enc = OHE(inputCols=["a"], outputCols=["b"])\n' in modified_code


def test_renames_one_hot_encoder_estimator_attribute_calls():
    given_code = """
import pyspark.ml.feature as F
enc = F.OneHotEncoderEstimator(inputCols=["a"], outputCols=["b"])
loaded = pyspark.ml.feature.OneHotEncoderEstimator.load("/path")
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    assert (
        'enc = F.OneHotEncoder(inputCols=["a"], outputCols=["b"])  # PY24-30-010:'
        in modified_code
    )
    assert (
        'loaded = pyspark.ml.feature.OneHotEncoder.load("/path")  # PY24-30-010:'
        in modified_code
    )
    assert "OneHotEncoderEstimator" not in modified_code.replace(
        "OneHotEncoderEstimator is renamed", ""
    )


def test_deduplicates_one_hot_encoder_import_after_rename():
    given_code = """
from pyspark.ml.feature import OneHotEncoder, OneHotEncoderEstimator, StringIndexer
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, OneHotEncoder
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    assert (
        "from pyspark.ml.feature import OneHotEncoder, StringIndexer  # PY24-30-010:"
        in modified_code
    )
    assert (
        "from pyspark.ml.feature import StringIndexer, OneHotEncoder  # PY24-30-010:"
        in modified_code
    )


def test_does_not_rename_one_hot_encoder_estimator_in_string_literals():
    given_code = """
name = "OneHotEncoderEstimator"
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    assert modified_code == given_code


def test_one_hot_encoder_estimator_rename_is_idempotent():
    given_code = """
from pyspark.ml.feature import OneHotEncoder, OneHotEncoderEstimator
enc = OneHotEncoderEstimator(inputCols=["a"], outputCols=["b"])
"""
    modified_once = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    modified_twice = rewrite(modified_once, OneHotEncoderEstimatorRenamed())
    assert modified_once == modified_twice


def test_adds_hint_for_one_hot_encoder_transform_without_fit():
    given_code = """
from pyspark.ml.feature import OneHotEncoder
out = OneHotEncoder(inputCols=["a"], outputCols=["b"]).transform(df)
"""
    modified_code = rewrite(given_code, OneHotEncoderTransformWithoutFit())
    expected_code = """
from pyspark.ml.feature import OneHotEncoder
out = OneHotEncoder(inputCols=["a"], outputCols=["b"]).transform(df)  # PY24-30-011: As of Spark 3.0, OneHotEncoder is an Estimator (the Transformer-style OneHotEncoder was removed); call OneHotEncoder(...).fit(df).transform(df) or use it inside a Pipeline.  # noqa: E501
"""
    assert modified_code == expected_code


def test_adds_hint_for_one_hot_encoder_transform_without_fit_via_module_attribute():
    given_code = """
from pyspark.ml import feature
import pyspark.ml.feature as mlf
a = feature.OneHotEncoder(inputCol="a", outputCol="b").transform(df)
b = mlf.OneHotEncoder(inputCol="a", outputCol="b").transform(df)
c = pyspark.ml.feature.OneHotEncoder(inputCol="a", outputCol="b").transform(df)
"""
    modified_code = rewrite(given_code, OneHotEncoderTransformWithoutFit())
    assert modified_code.count("# PY24-30-011:") == 3


def test_does_nothing_for_one_hot_encoder_transform_from_sklearn_or_with_fit_or_without_import():
    given_code = """
from sklearn.preprocessing import OneHotEncoder
out = OneHotEncoder(handle_unknown="ignore").transform(X)
"""
    assert rewrite(given_code, OneHotEncoderTransformWithoutFit()) == given_code

    given_code = """
from pyspark.ml.feature import OneHotEncoder
from sklearn.preprocessing import OneHotEncoder
out = OneHotEncoder(handle_unknown="ignore").transform(X)
"""
    assert rewrite(given_code, OneHotEncoderTransformWithoutFit()) == given_code

    given_code = """
from pyspark.ml.feature import OneHotEncoder
out = OneHotEncoder(inputCols=["a"], outputCols=["b"]).fit(df).transform(df)
"""
    assert rewrite(given_code, OneHotEncoderTransformWithoutFit()) == given_code

    given_code = """
out = OneHotEncoder(inputCols=["a"], outputCols=["b"]).transform(df)
"""
    assert rewrite(given_code, OneHotEncoderTransformWithoutFit()) == given_code


def test_does_not_flag_datetime_patterns_on_non_spark_receivers():
    given_code = """
import pyspark.sql.functions as F
x = arrow.get().to_date(a, 'YYYY')
y = pdf.to_timestamp('M', 'end')
z = period_idx.to_timestamp('Y', 'start')
v = my_obj.date_format(c, 'YYYY')
w = date_format(c, 'YYYY')
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    assert modified_code == given_code


def test_flags_datetime_patterns_through_every_functions_module_binding():
    given_code = """
from pyspark.sql import functions
from pyspark.sql import functions as sf
from pyspark.sql.functions import to_date as td
a = functions.date_format(c, 'YYYY')
b = sf.to_timestamp(c, 'YYYY')
c2 = pyspark.sql.functions.unix_timestamp(c, 'YYYY')
d = td(c, 'YYYY')
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    assert modified_code.count("# PY24-30-009:") == 4


def test_datetime_pattern_with_line_break_does_not_break_the_hint_comment():
    given_code = """
import pyspark.sql.functions as F
a = F.date_format(c, 'YYYY\\nMM')
b = F.date_format(c, '''YYYY
MM''')
c2 = F.date_format(c, 'YYYY # x')
d = F.date_format(c, 'YYYY\\rMM')
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    assert modified_code.count("# PY24-30-009:") == 4
    assert "'YYYY\\nMM' (letter(s) Y)" in modified_code
    assert "'YYYY\\rMM' (letter(s) Y)" in modified_code
    compile(modified_code, "<test>", "exec")


def test_several_datetime_patterns_on_one_line_produce_a_single_message():
    given_code = """
import pyspark.sql.functions as F
r = spark.read.option("dateFormat", "YYYY").csv(p).withColumn("x", F.date_format(c, "ww"))
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    assert modified_code.count("# PY24-30-009:") == 1
    assert (
        "Datetime patterns 'YYYY' (letter(s) Y), 'ww' (letter(s) w) use letters"
        in modified_code
    )
    assert modified_code.count("DateTimeFormatter replaced SimpleDateFormat") == 1


def test_datetime_pattern_hint_lands_on_the_compound_statement_header():
    given_code = """
import pyspark.sql.functions as F
if F.date_format(c, 'YYYY') == '2020':
    y = 1
for row in df.select(F.to_date(c, 'YYYY')).collect():
    pass
@decorator(F.to_date(c, 'YYYY'))
def g(c=F.to_date(c, 'ww')):
    '''doc'''
class K(F.to_date(c, 'YYYY')):
    z = 2
"""
    modified_code = rewrite(given_code, DateTimePatternLettersChanged())
    lines = modified_code.splitlines()
    assert lines[2].startswith("if F.date_format(c, 'YYYY') == '2020':  # PY24-30-009:")
    assert lines[3] == "    y = 1"
    assert lines[4].startswith(
        "for row in df.select(F.to_date(c, 'YYYY')).collect():  # PY24-30-009:"
    )
    assert lines[5] == "    pass"
    assert lines[6] == "@decorator(F.to_date(c, 'YYYY'))"
    assert lines[7].startswith("def g(c=F.to_date(c, 'ww')):  # PY24-30-009:")
    assert "'YYYY' (letter(s) Y), 'ww' (letter(s) w)" in lines[7]
    assert lines[8] == "    '''doc'''"
    assert lines[9].startswith("class K(F.to_date(c, 'YYYY')):  # PY24-30-009:")
    assert lines[10] == "    z = 2"


def test_does_not_rename_one_hot_encoder_estimator_names_of_other_origin():
    given_code = """
from sklearn.preprocessing import OneHotEncoder
import mylib

class OneHotEncoderEstimator(Wrapper):
    pass

x = mylib.OneHotEncoderEstimator()
y = f(OneHotEncoderEstimator=1)
z = obj.OneHotEncoderEstimator
doc = "use OneHotEncoderEstimator"
import OneHotEncoderEstimator
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    assert modified_code == given_code


def test_does_not_rename_keyword_arguments_even_when_the_estimator_is_imported():
    given_code = """
from pyspark.ml.feature import OneHotEncoderEstimator
stages = build(OneHotEncoderEstimator=OneHotEncoderEstimator(inputCols=["a"], outputCols=["b"]))
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    assert (
        'stages = build(OneHotEncoderEstimator=OneHotEncoder(inputCols=["a"], outputCols=["b"]))  # PY24-30-010:'
        in modified_code
    )


def test_renames_one_hot_encoder_estimator_in_compound_statement_headers():
    given_code = """
from pyspark.ml.feature import OneHotEncoderEstimator
class MyEnc(OneHotEncoderEstimator):
    def __init__(self):
        pass
def f(e: OneHotEncoderEstimator) -> OneHotEncoderEstimator:
    return e
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    lines = modified_code.splitlines()
    assert lines[2].startswith("class MyEnc(OneHotEncoder):  # PY24-30-010:")
    assert lines[3] == "    def __init__(self):"
    assert lines[5].startswith(
        "def f(e: OneHotEncoder) -> OneHotEncoder:  # PY24-30-010:"
    )
    assert lines[6] == "    return e"


def test_deduplicated_one_hot_encoder_import_keeps_inline_comments_and_layout():
    given_code = """
from pyspark.ml.feature import (
    OneHotEncoder,  # legacy transformer
    OneHotEncoderEstimator,  # new estimator
    StringIndexer,
)
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoderEstimator,
    OneHotEncoder,
)
from pyspark.ml.feature import (
    OneHotEncoderEstimator,
    OneHotEncoder
)
from pyspark.ml.feature import (
    OneHotEncoderEstimator,  # keep me
    OneHotEncoder
)
from pyspark.ml.feature import (StringIndexer, OneHotEncoder, OneHotEncoderEstimator,  # tail
)
"""
    modified_code = rewrite(given_code, OneHotEncoderEstimatorRenamed())
    hint = "# PY24-30-010: As of Spark 3.0, OneHotEncoderEstimator is renamed to OneHotEncoder; the old OneHotEncoder Transformer was removed.  # noqa: E501"
    expected_code = f"""
from pyspark.ml.feature import (
    OneHotEncoder,  # legacy transformer  # new estimator
    StringIndexer,
)  {hint}
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
)  {hint}
from pyspark.ml.feature import (
    OneHotEncoder
)  {hint}
from pyspark.ml.feature import (
    OneHotEncoder,  # keep me
)  {hint}
from pyspark.ml.feature import (StringIndexer, OneHotEncoder,  # tail
)  {hint}
"""
    assert modified_code == expected_code
    compile(modified_code, "<test>", "exec")


def test_adds_hint_for_two_step_one_hot_encoder_transform_without_fit():
    given_code = """
from pyspark.ml.feature import OneHotEncoder
enc = OneHotEncoder(inputCols=["a"], outputCols=["b"])
out = enc.transform(df)
"""
    modified_code = rewrite(given_code, OneHotEncoderTransformWithoutFit())
    assert 'enc = OneHotEncoder(inputCols=["a"], outputCols=["b"])\n' in modified_code
    assert "out = enc.transform(df)  # PY24-30-011:" in modified_code


def test_does_nothing_for_two_step_transform_when_the_variable_is_fitted_or_rebound():
    given_code = """
from pyspark.ml.feature import OneHotEncoder
enc = OneHotEncoder(inputCols=["a"], outputCols=["b"])
model = enc.fit(df)
out = model.transform(df)
enc2 = OneHotEncoder(inputCols=["a"], outputCols=["b"])
enc2 = enc2.fit(df)
out2 = enc2.transform(df)
for enc3 in encoders:
    out3 = enc3.transform(df)
"""
    modified_code = rewrite(given_code, OneHotEncoderTransformWithoutFit())
    assert modified_code == given_code
