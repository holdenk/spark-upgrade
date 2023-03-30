import pyspark
import pandas as pd

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
