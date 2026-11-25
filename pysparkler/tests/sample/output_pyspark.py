import pyspark  # PY35-40-010: As of PySpark 4.0, Python 3.8 support has been dropped, Python 3.9 or higher is required.  # PY40-41-003: As of PySpark 4.1, Python 3.9 support has been dropped, Python 3.10 or higher is required.  # noqa: E501
import numpy as np  # PY35-40-002: PySpark 4.0 requires numpy version 1.21 or higher  # noqa: E501
import pandas as pd  # PY22-23-001: PySpark 2.3 requires pandas version 0.19.2 or higher  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher  # PY35-40-001: PySpark 4.0 requires pandas version 2.0.0 or higher  # PY40-41-002: PySpark 4.1 requires pandas version 2.2.0 or higher  # noqa: E501
import pyspark.pandas as ps  # PY22-23-001: PySpark 2.3 requires pandas version 0.19.2 or higher  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher  # PY35-40-001: PySpark 4.0 requires pandas version 2.0.0 or higher  # PY35-40-007: As of PySpark 4.0, ANSI mode is enabled by default and pandas API on Spark raises an exception under ANSI mode. Disable it via spark.sql.ansi.enabled=false, or set the pandas-on-spark option compute.fail_on_ansi_mode=False to force it to work.  # PY40-41-002: PySpark 4.1 requires pandas version 2.2.0 or higher  # noqa: E501

from pandas import DataFrame as df  # PY22-23-001: PySpark 2.3 requires pandas version 0.19.2 or higher  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher  # PY32-33-002: PySpark 3.3 requires pandas version 1.0.5 or higher  # PY35-40-001: PySpark 4.0 requires pandas version 2.0.0 or higher  # PY40-41-002: PySpark 4.1 requires pandas version 2.2.0 or higher  # noqa: E501
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import pandas_udf, PandasUDFType  # PY24-30-003: PySpark 3.0 requires PyArrow version 0.12.1 or higher to use pandas_udf  # PY35-40-003: PySpark 4.0 requires PyArrow version 11.0.0 or higher to use pandas_udf  # PY40-41-001: PySpark 4.1 requires PyArrow version 15.0.0 or higher to use pandas_udf  # PY41-42-001: PySpark 4.2 requires PyArrow version 18.0.0 or higher to use pandas_udf  # noqa: E501
from pyspark.ml.param.shared import *  # PY24-30-008: In Spark 3.0, pyspark.ml.param.shared.Has* mixins do not provide any set*(self, value) setter methods anymore, use the respective self.set(self.*, value) instead.  # noqa: E501

spark = SparkSession.builder.appName('example').getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")  # PY24-30-004: PySpark 3.0 requires PyArrow version 0.12.1 or higher when spark.sql.execution.arrow.enabled is set to true  # PY24-30-005: Consider setting spark.sql.execution.pandas.convertToArrowArraySafely to true to raise errors in case of Integer overflow or Floating point truncation, instead of silent allows.  # noqa: E501

table_name = "my_table"
result = spark.sql(f"select int(dateint) val from {table_name} limit 10")  # PY21-33-001: Spark SQL statement has been upgraded to Spark 3.3 compatible syntax.  # noqa: E501

data = [("James", "", "Smith", "36636", "M", 60000),
        ("Jen", "Mary", "Brown", "", "F", 0)]

columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
pysparkDF = spark.createDataFrame(data=data, schema=columns, verifySchema=True)  # PY24-30-006: Setting verifySchema to True validates LongType as well in PySpark 3.0. Previously, LongType was not verified and resulted in None in case the value overflows.  # PY40-41-005: As of PySpark 4.1, spark.sql.execution.pandas.convertToArrowArraySafely is enabled by default. If this call converts pandas data (creating a DataFrame from a pandas DataFrame, or an Arrow-enabled UDF), PyArrow now raises errors on unsafe conversions (integer overflow, float truncation, loss of precision); it has no effect on createDataFrame calls built from non-pandas data such as Python lists or RDDs. To restore the previous behavior, set it to false.  # noqa: E501

pandasDF = pysparkDF.toPandas()  # PY23-24-001: As of PySpark 2.4 toPandas() allows fallback to non-optimization by default when Arrow optimization is unable to be used. This can be switched off by spark.sql.execution.arrow.fallback.enabled  # PY24-30-002: PySpark 3.0 requires a pandas version of 0.23.2 or higher to use toPandas()  # PY41-42-002: As of PySpark 4.2, columnar data exchange between PySpark and the JVM uses Apache Arrow by default (spark.sql.execution.arrow.pyspark.enabled defaults to true). To restore the legacy row-based exchange, set it to false.  # noqa: E501
print(pandasDF)

pysparkDF.write.partitionBy('gender').saveAsTable("persons")  # PY23-24-002: As of PySpark 2.4 the new DataFrameWriterV2 API is recommended for creating or replacing tables using data frames. To run a CTAS or RTAS, use create(), replace(), or createOrReplace() operations. For example: df.writeTo("prod.db.table").partitionedBy("dateint").createOrReplace(). Please note that the v1 DataFrame write API is still supported, but is not recommended.  # noqa: E501
pysparkDF.write.insertInto("persons", overwrite=True)  # PY23-24-003: As of PySpark 2.4 the new DataFrameWriterV2 API is recommended for writing into tables in append or overwrite mode. For example, to append use df.writeTo(t).append() and to overwrite partitions dynamically use df.writeTo(t).overwritePartitions() Please note that the v1 DataFrame write API is still supported, but is not recommended.  # noqa: E501

data = [Row(lang=["Java", "Scala", "C++"], name="James,,Smith", state="CA"),
        Row(lang=["CSharp", "VB"], name="Robert,,Williams", state="NV")]  # PY24-30-007: Sorting Row fields by name alphabetically since as of Spark 3.0, they are no longer when constructed with named arguments.  # noqa: E501

rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

ps_df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
ps_df.drop(['B', 'C'])  # PY32-33-001: As of PySpark 3.3 the drop method of pandas API on Spark DataFrame sets drop by index as default, instead of drop by column. Please explicitly set axis argument to 1 to drop by column.  # noqa: E501

a_column_values = list(ps_df['A'].unique())
repr_a_column_values = [repr(value) for value in a_column_values]  # PY32-33-003: As of PySpark 3.3, the repr return values of SQL DataTypes have been changed to yield an object with the same value when passed to eval.  # noqa: E501

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")  # PY22-23-002: As of PySpark 2.3 the behavior of timestamp values for Pandas related functionalities was changed to respect session timezone. If you want to use the old behavior, you need to set a configuration spark.sql.execution.pandas.respectSessionTimeZone to False.  # noqa: E501
tz_df = spark.createDataFrame([28801], "long").selectExpr("timestamp(value) as ts")  # PY40-41-005: As of PySpark 4.1, spark.sql.execution.pandas.convertToArrowArraySafely is enabled by default. If this call converts pandas data (creating a DataFrame from a pandas DataFrame, or an Arrow-enabled UDF), PyArrow now raises errors on unsafe conversions (integer overflow, float truncation, loss of precision); it has no effect on createDataFrame calls built from non-pandas data such as Python lists or RDDs. To restore the previous behavior, set it to false.  # noqa: E501
tz_df.show()

rp_df = spark.createDataFrame([
        (10, 80.5, "Alice", None),
        (5, None, "Bob", None),
        (None, None, "Tom", None),
        (None, None, None, True)],
        schema=["age", "height", "name", "bool"])  # PY40-41-005: As of PySpark 4.1, spark.sql.execution.pandas.convertToArrowArraySafely is enabled by default. If this call converts pandas data (creating a DataFrame from a pandas DataFrame, or an Arrow-enabled UDF), PyArrow now raises errors on unsafe conversions (integer overflow, float truncation, loss of precision); it has no effect on createDataFrame calls built from non-pandas data such as Python lists or RDDs. To restore the previous behavior, set it to false.  # noqa: E501

rp_df.na.replace('Alice').show()  # PY22-23-003: As of PySpark 2.3, df.replace does not allow to omit value when to_replace is not a dictionary. Previously, value could be omitted in the other cases and had None by default, which is counterintuitive and error-prone.  # noqa: E501
rp_df.na.fill(False).show()  # PY22-23-004: As of PySpark 2.3, na.fill() or fillna also accepts boolean and replaces nulls with booleans. In prior Spark versions, PySpark just ignores it and returns the original Dataset/DataFrame.  # noqa: E501
rp_df.fillna(True).show()  # PY22-23-004: As of PySpark 2.3, na.fill() or fillna also accepts boolean and replaces nulls with booleans. In prior Spark versions, PySpark just ignores it and returns the original Dataset/DataFrame.  # noqa: E501


def truncate(truncate=True):
        try:
                int_truncate = int(truncate)
        except ValueError as ex:
                raise TypeError(
                        "Parameter 'truncate={}' should be either bool or int.".format(truncate)
                )  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.  # noqa: E501
