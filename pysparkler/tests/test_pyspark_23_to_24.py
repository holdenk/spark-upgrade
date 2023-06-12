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
from pysparkler.pyspark_23_to_24 import (
    RecommendDataFrameWriterV2ApiForV1ApiInsertInto,
    RecommendDataFrameWriterV2ApiForV1ApiSaveAsTable,
    ToPandasAllowsFallbackOnArrowOptimization,
)
from tests.conftest import rewrite


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
    modified_code = rewrite(given_code, ToPandasAllowsFallbackOnArrowOptimization())
    expected_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)

pandasDF = pysparkDF.toPandas()  # PY23-24-001: As of PySpark 2.4 toPandas() allows fallback to non-optimization by default when Arrow optimization is unable to be used. This can be switched off by spark.sql.execution.arrow.fallback.enabled  # noqa: E501
print(pandasDF)
"""
    assert modified_code == expected_code


def test_writes_comment_when_data_frame_writer_v1_api_save_as_table_is_detected():
    given_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)

pysparkDF.write.partitionBy('gender').saveAsTable("persons")
"""
    modified_code = rewrite(
        given_code, RecommendDataFrameWriterV2ApiForV1ApiSaveAsTable()
    )
    expected_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)

pysparkDF.write.partitionBy('gender').saveAsTable("persons")  # PY23-24-002: As of PySpark 2.4 the new DataFrameWriterV2 API is recommended for creating or replacing tables using data frames. To run a CTAS or RTAS, use create(), replace(), or createOrReplace() operations. For example: df.writeTo("prod.db.table").partitionedBy("dateint").createOrReplace(). Please note that the v1 DataFrame write API is still supported, but is not recommended.  # noqa: E501
"""
    assert modified_code == expected_code


def test_writes_comment_when_data_frame_writer_v1_api_insert_into_is_detected():
    given_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)

pysparkDF.write.insertInto("persons", overwrite=True)
"""
    modified_code = rewrite(
        given_code, RecommendDataFrameWriterV2ApiForV1ApiInsertInto()
    )
    expected_code = """\
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('example').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)

pysparkDF.write.insertInto("persons", overwrite=True)  # PY23-24-003: As of PySpark 2.4 the new DataFrameWriterV2 API is recommended for writing into tables in append or overwrite mode. For example, to append use df.writeTo(t).append() and to overwrite partitions dynamically use df.writeTo(t).overwritePartitions() Please note that the v1 DataFrame write API is still supported, but is not recommended.  # noqa: E501
"""
    assert modified_code == expected_code
