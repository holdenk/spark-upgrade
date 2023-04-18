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
from pysparkler.pyspark_23_to_24 import ToPandasAllowsFallbackOnArrowOptimization
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
