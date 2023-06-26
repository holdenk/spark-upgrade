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

from pysparkler.sql_21_to_33 import SqlStatementUpgradeAndCommentWriter
from tests.conftest import rewrite


def test_upgrades_non_templated_sql():
    given_code = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL Example").getOrCreate()
result = spark.sql("select cast(dateint as int) val from my_table limit 10")
spark.stop()
"""
    modified_code = rewrite(given_code, SqlStatementUpgradeAndCommentWriter())
    expected_code = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL Example").getOrCreate()
result = spark.sql("select int(dateint) val from my_table limit 10")  # PY21-33-001: Please note, PySparkler makes a best effort to upcast SQL statements directly being executed. However, the upgrade won't be possible for certain templated SQLs, and in those scenarios please de-template the SQL and use the Sqlfluff tooling to upcast the SQL yourself.  # noqa: E501
spark.stop()
"""
    assert modified_code == expected_code
