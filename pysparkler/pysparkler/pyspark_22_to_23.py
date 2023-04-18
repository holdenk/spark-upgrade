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
import libcst as cst
import libcst.matchers as m

from pysparkler.base import (
    RequiredDependencyVersionCommentWriter,
    StatementLineCommentWriter,
    one_of_matching_strings,
)


class RequiredPandasVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In PySpark 2.3, now we need Pandas 0.19.2 or upper if you want to use Pandas related functionalities, such as
    toPandas, createDataFrame from Pandas DataFrame, etc."""

    def __init__(
        self,
        pyspark_version: str = "2.3",
        required_dependency_name: str = "pandas",
        required_dependency_version: str = "0.19.2",
    ):
        super().__init__(
            transformer_id="PY22-23-001",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
        )


class PandasRespectSessionTimeZone(StatementLineCommentWriter):
    """In PySpark 2.3, the behavior of timestamp values for Pandas related functionalities was changed to respect
    session timezone. If you want to use the old behavior, you need to set a configuration
    spark.sql.execution.pandas.respectSessionTimeZone to False. See SPARK-22395 for details.
    """

    def __init__(
        self,
        pyspark_version: str = "2.3",
    ):
        super().__init__(
            transformer_id="PY22-23-002",
            comment=f"As of PySpark {pyspark_version} the behavior of timestamp values for Pandas related \
functionalities was changed to respect session timezone. If you want to use the old behavior, you need to set a \
configuration spark.sql.execution.pandas.respectSessionTimeZone to False.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if spark_session.conf.set("spark.sql.session.timeZone", "some_timezone")"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Attribute(attr=m.Name("set")),
                    m.Attribute(attr=m.Name("config")),
                ),
                args=[
                    m.Arg(value=one_of_matching_strings("spark.sql.session.timeZone")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class DataFrameReplaceWithoutUsingDictionary(StatementLineCommentWriter):
    """In PySpark 2.3, df.replace does not allow to omit value when to_replace is not a dictionary. Previously, value
    could be omitted in the other cases and had None by default, which is counterintuitive and error-prone.
    """

    def __init__(
        self,
        pyspark_version: str = "2.3",
    ):
        super().__init__(
            transformer_id="PY22-23-003",
            comment=f"As of PySpark {pyspark_version}, df.replace does not allow to omit value when to_replace is not \
a dictionary. Previously, value could be omitted in the other cases and had None by default, which is counterintuitive \
and error-prone.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if df.na.replace is being called on a DataFrame with only one argument that is not a dictionary"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(
                    value=m.Attribute(attr=m.Name("na")), attr=m.Name("replace")
                ),
                args=[m.AtMostN(n=1, matcher=m.DoesNotMatch(m.Arg(m.Dict())))],
            ),
        ):
            self.match_found = True


class FillNaReplacesBooleanWithNulls(StatementLineCommentWriter):
    """In PySpark 2.3, na.fill() or fillna also accepts boolean and replaces nulls with booleans. In prior Spark
    versions, PySpark just ignores it and returns the original Dataset/DataFrame.
    """

    def __init__(
        self,
        pyspark_version: str = "2.3",
    ):
        super().__init__(
            transformer_id="PY22-23-004",
            comment=f"As of PySpark {pyspark_version}, na.fill() or fillna also accepts boolean and replaces nulls \
with booleans. In prior Spark versions, PySpark just ignores it and returns the original Dataset/DataFrame.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if df.na.fill or fillna is being called on a DataFrame with only one argument that is a boolean"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Attribute(
                        value=m.Attribute(attr=m.Name("na")), attr=m.Name("fill")
                    ),
                    m.Attribute(attr=m.Name("fillna")),
                ),
                args=[
                    m.AtMostN(
                        n=1,
                        matcher=m.Arg(value=m.OneOf(m.Name("True"), m.Name("False"))),
                    )
                ],
            ),
        ):
            self.match_found = True


def pyspark_22_to_23_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 2.2 to 2.3 migration guide"""
    return [
        RequiredPandasVersionCommentWriter(),
        PandasRespectSessionTimeZone(),
        DataFrameReplaceWithoutUsingDictionary(),
        FillNaReplacesBooleanWithNulls(),
    ]
