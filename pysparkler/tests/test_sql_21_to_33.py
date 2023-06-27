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
result = spark.sql("select int(dateint) val from my_table limit 10")  # PY21-33-001: Spark SQL statement has been upgraded to Spark 3.3 compatible syntax.  # noqa: E501
spark.stop()
"""
    assert modified_code == expected_code


def test_upgrades_templated_sql():
    given_code = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL Example").getOrCreate()
table_name = "my_table"
result = spark.sql(f"select cast(dateint as int) val from {table_name} limit 10")
spark.stop()
"""
    modified_code = rewrite(given_code, SqlStatementUpgradeAndCommentWriter())
    expected_code = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL Example").getOrCreate()
table_name = "my_table"
result = spark.sql(f"select int(dateint) val from {table_name} limit 10")  # PY21-33-001: Spark SQL statement has been upgraded to Spark 3.3 compatible syntax.  # noqa: E501
spark.stop()
"""
    assert modified_code == expected_code


def test_unable_to_upgrade_templated_sql_with_complex_expressions():
    given_code = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL Example").getOrCreate()
table_name = "my_table"
num = 10
result = spark.sql(f"select cast(dateint as int) val from {table_name} where x < {num * 100} limit 10")
spark.stop()
"""
    modified_code = rewrite(given_code, SqlStatementUpgradeAndCommentWriter())
    expected_code = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL Example").getOrCreate()
table_name = "my_table"
num = 10
result = spark.sql(f"select cast(dateint as int) val from {table_name} where x < {num * 100} limit 10")  # PY21-33-001: Unable to inspect the Spark SQL statement since the formatted string SQL has complex expressions within. Please de-template the SQL and use the Sqlfluff tooling to upcast the SQL yourself.  # noqa: E501
spark.stop()
"""
    assert modified_code == expected_code


def test_no_upgrades_required_after_inspecting_sql():
    given_code = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL Example").getOrCreate()
result = spark.sql("select * from my_table limit 10")
spark.stop()
"""
    modified_code = rewrite(given_code, SqlStatementUpgradeAndCommentWriter())
    expected_code = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL Example").getOrCreate()
result = spark.sql("select * from my_table limit 10")  # PY21-33-001: Spark SQL statement has Spark 3.3 compatible syntax.  # noqa: E501
spark.stop()
"""
    assert modified_code == expected_code
