import pyspark
import numpy as np
import pandas as pd
import pyspark.pandas as ps

from pandas import DataFrame as df
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.ml.param.shared import *

spark = SparkSession.builder.appName('example').getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

data = [("James", "", "Smith", "36636", "M", 60000),
        ("Jen", "Mary", "Brown", "", "F", 0)]

columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
pysparkDF = spark.createDataFrame(data=data, schema=columns, verifySchema=True)

pandasDF = pysparkDF.toPandas()
print(pandasDF)

data = [Row(name="James,,Smith", lang=["Java", "Scala", "C++"], state="CA"),
        Row(name="Robert,,Williams", lang=["CSharp", "VB"], state="NV")]

rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

ps_df = ps.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
ps_df.drop(['B', 'C'])

a_column_values = list(ps_df['A'].unique())
repr_a_column_values = [repr(value) for value in a_column_values]

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
tz_df = spark.createDataFrame([28801], "long").selectExpr("timestamp(value) as ts")
tz_df.show()

rp_df = spark.createDataFrame([
        (10, 80, "Alice"),
        (5, None, "Bob"),
        (None, 10, "Tom"),
        (None, None, None)],
        schema=["age", "height", "name"])

rp_df.na.replace('Alice').show()


def truncate(truncate=True):
        try:
                int_truncate = int(truncate)
        except ValueError as ex:
                raise TypeError(
                        "Parameter 'truncate={}' should be either bool or int.".format(truncate)
                )
