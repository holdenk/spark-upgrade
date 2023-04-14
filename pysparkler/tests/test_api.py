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
from pysparkler.api import PySparkler
from tests.conftest import absolute_path


def test_upgrade_pyspark_python_script():
    modified_code = PySparkler(dry_run=True).upgrade_script(
        absolute_path("tests/sample_inputs/sample_pyspark.py")
    )
    expected_code = """\
import pyspark
import numpy as np
import pandas as pd  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher
import pyspark.pandas as ps  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher

from pandas import DataFrame as df  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import pandas_udf, PandasUDFType  # PY24-30-003: PySpark 3.0 requires PyArrow version 0.12.1 or higher to use pandas_udf
from pyspark.ml.param.shared import *  # PY24-30-008: In Spark 3.0, pyspark.ml.param.shared.Has* mixins do not provide any set*(self, value) setter methods anymore, use the respective self.set(self.*, value) instead.

spark = SparkSession.builder.appName('example').getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")  # PY24-30-004: PySpark 3.0 requires PyArrow version 0.12.1 or higher when spark.sql.execution.arrow.enabled is set to true  # PY24-30-005: Consider setting spark.sql.execution.pandas.convertToArrowArraySafely to true to raise errors in case of Integer overflow or Floating point truncation, instead of silent allows.

data = [("James", "", "Smith", "36636", "M", 60000),
        ("Jen", "Mary", "Brown", "", "F", 0)]

columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
pysparkDF = spark.createDataFrame(data=data, schema=columns, verifySchema=True)  # PY24-30-006: Setting verifySchema to True validates LongType as well in PySpark 3.0. Previously, LongType was not verified and resulted in None in case the value overflows.

pandasDF = pysparkDF.toPandas()  # PY23-24-001: As of PySpark 2.4 toPandas() allows fallback to non-optimization by default when Arrow optimization is unable to be used. This can be switched off by spark.sql.execution.arrow.fallback.enabled  # PY24-30-002: PySpark 3.0 requires a pandas version of 0.23.2 or higher to use toPandas()
print(pandasDF)

data = [Row(lang=["Java", "Scala", "C++"], name="James,,Smith", state="CA"),
        Row(lang=["CSharp", "VB"], name="Robert,,Williams", state="NV")]  # PY24-30-007: Sorting Row fields by name alphabetically since as of Spark 3.0, they are no longer when constructed with named arguments.

rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

ps_df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
ps_df.drop(['B', 'C'], axis = 1)  # PY32-33-001: As of PySpark 3.3, the drop method of pandas API on Spark DataFrame supports dropping rows by index, and sets dropping by index instead of column by default.

a_column_values = list(ps_df['A'].unique())
repr_a_column_values = [repr(value) for value in a_column_values]  # PY32-33-003: As of PySpark 3.3, the repr return values of SQL DataTypes have been changed to yield an object with the same value when passed to eval.


def truncate(truncate=True):
        try:
                int_truncate = int(truncate)
        except ValueError as ex:
                raise TypeError(
                        "Parameter 'truncate={}' should be either bool or int.".format(truncate)
                )  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.
"""
    assert modified_code == expected_code


def test_upgrade_pyspark_jupyter_notebook():
    modified_code = PySparkler(dry_run=True).upgrade_notebook(
        absolute_path("tests/sample_inputs/SamplePySparkNotebook.ipynb"),
        output_kernel_name="spark33-python3-venv",
    )
    expected_code = r"""{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "b5e40a68",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pyspark\n",
    "import numpy as np\n",
    "import pandas as pd  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher\n",
    "import pyspark.pandas as ps  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher\n",
    "\n",
    "from pandas import DataFrame as df  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher\n",
    "from pyspark.sql import SparkSession, Row\n",
    "from pyspark.sql.functions import pandas_udf, PandasUDFType  # PY24-30-003: PySpark 3.0 requires PyArrow version 0.12.1 or higher to use pandas_udf\n",
    "from pyspark.ml.param.shared import *  # PY24-30-008: In Spark 3.0, pyspark.ml.param.shared.Has* mixins do not provide any set*(self, value) setter methods anymore, use the respective self.set(self.*, value) instead.\n",
    "\n",
    "spark = SparkSession.builder.appName('example').getOrCreate()\n",
    "spark.conf.set(\"spark.sql.execution.arrow.enabled\", \"true\")  # PY24-30-004: PySpark 3.0 requires PyArrow version 0.12.1 or higher when spark.sql.execution.arrow.enabled is set to true  # PY24-30-005: Consider setting spark.sql.execution.pandas.convertToArrowArraySafely to true to raise errors in case of Integer overflow or Floating point truncation, instead of silent allows.\n",
    "\n",
    "data = [(\"James\", \"\", \"Smith\", \"36636\", \"M\", 60000),\n",
    "        (\"Jen\", \"Mary\", \"Brown\", \"\", \"F\", 0)]\n",
    "\n",
    "columns = [\"first_name\", \"middle_name\", \"last_name\", \"dob\", \"gender\", \"salary\"]\n",
    "pysparkDF = spark.createDataFrame(data=data, schema=columns, verifySchema=True)  # PY24-30-006: Setting verifySchema to True validates LongType as well in PySpark 3.0. Previously, LongType was not verified and resulted in None in case the value overflows.\n",
    "\n",
    "pandasDF = pysparkDF.toPandas()  # PY23-24-001: As of PySpark 2.4 toPandas() allows fallback to non-optimization by default when Arrow optimization is unable to be used. This can be switched off by spark.sql.execution.arrow.fallback.enabled  # PY24-30-002: PySpark 3.0 requires a pandas version of 0.23.2 or higher to use toPandas()\n",
    "print(pandasDF)\n",
    "\n",
    "data = [Row(lang=[\"Java\", \"Scala\", \"C++\"], name=\"James,,Smith\", state=\"CA\"),\n",
    "        Row(lang=[\"CSharp\", \"VB\"], name=\"Robert,,Williams\", state=\"NV\")]  # PY24-30-007: Sorting Row fields by name alphabetically since as of Spark 3.0, they are no longer when constructed with named arguments.\n",
    "\n",
    "rdd = spark.sparkContext.parallelize(data)\n",
    "print(rdd.collect())\n",
    "\n",
    "ps_df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])\n",
    "ps_df.drop(['B', 'C'], axis = 1)  # PY32-33-001: As of PySpark 3.3, the drop method of pandas API on Spark DataFrame supports dropping rows by index, and sets dropping by index instead of column by default.\n",
    "\n",
    "a_column_values = list(ps_df['A'].unique())\n",
    "repr_a_column_values = [repr(value) for value in a_column_values]  # PY32-33-003: As of PySpark 3.3, the repr return values of SQL DataTypes have been changed to yield an object with the same value when passed to eval.\n",
    "\n",
    "\n",
    "def truncate(truncate=True):\n",
    "        try:\n",
    "                int_truncate = int(truncate)\n",
    "        except ValueError as ex:\n",
    "                raise TypeError(\n",
    "                        \"Parameter 'truncate={}' should be either bool or int.\".format(truncate)\n",
    "                )  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.\n"
   ]
  }
 ],
 "metadata": {
  "hide_input": false,
  "kernelspec": {
   "display_name": "spark33-python3-venv",
   "language": "python",
   "name": "spark33-python3-venv"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}"""
    assert modified_code == expected_code
