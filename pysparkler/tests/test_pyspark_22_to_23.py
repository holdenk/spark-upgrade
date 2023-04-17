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

from pysparkler.pyspark_22_to_23 import (
    PandasRespectSessionTimeZone,
    RequiredPandasVersionCommentWriter,
)
from tests.conftest import rewrite


def test_adds_required_pandas_version_comment_to_import_statements():
    given_code = """
import pandas
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas  # PY22-23-001: PySpark 2.3 requires pandas version 0.19.2 or higher
import pyspark
"""
    assert modified_code == expected_code


def test_adds_pandas_respects_session_timezone_comment_when_session_timezone_config_is_being_set():
    given_code = """
import pandas
import pyspark

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
df = spark.createDataFrame([28801], "long").selectExpr("timestamp(value) as ts")
df.show()

df.toPandas()
"""
    modified_code = rewrite(given_code, PandasRespectSessionTimeZone())
    expected_code = """
import pandas
import pyspark

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")  # PY22-23-002: As of PySpark 2.3 the behavior of timestamp values for Pandas related functionalities was changed to respect session timezone. If you want to use the old behavior, you need to set a configuration spark.sql.execution.pandas.respectSessionTimeZone to False.
df = spark.createDataFrame([28801], "long").selectExpr("timestamp(value) as ts")
df.show()

df.toPandas()
"""
    assert modified_code == expected_code
