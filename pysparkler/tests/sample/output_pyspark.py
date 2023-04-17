import pyspark
import numpy as np
import pandas as pd  # PY22-23-001: PySpark 2.3 requires pandas version 0.19.2 or higher  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher
import pyspark.pandas as ps  # PY22-23-001: PySpark 2.3 requires pandas version 0.19.2 or higher  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher

from pandas import DataFrame as df  # PY22-23-001: PySpark 2.3 requires pandas version 0.19.2 or higher  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher
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

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")  # PY22-23-002: As of PySpark 2.3 the behavior of timestamp values for Pandas related functionalities was changed to respect session timezone. If you want to use the old behavior, you need to set a configuration spark.sql.execution.pandas.respectSessionTimeZone to False.
tz_df = spark.createDataFrame([28801], "long").selectExpr("timestamp(value) as ts")
tz_df.show()

rp_df = spark.createDataFrame([
        (10, 80, "Alice"),
        (5, None, "Bob"),
        (None, 10, "Tom"),
        (None, None, None)],
        schema=["age", "height", "name"])

rp_df.na.replace('Alice').show()  # PY22-23-003: As of PySpark 2.3, df.replace does not allow to omit value when to_replace is not a dictionary. Previously, value could be omitted in the other cases and had None by default, which is counterintuitive and error-prone.


def truncate(truncate=True):
        try:
                int_truncate = int(truncate)
        except ValueError as ex:
                raise TypeError(
                        "Parameter 'truncate={}' should be either bool or int.".format(truncate)
                )  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.
