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

# Migration rules for PySpark 2.4 to 3.0
# https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_2.4_to_3.0.html


class RequiredPandasVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In Spark 3.0, PySpark requires a pandas version of 0.23.2 or higher to use pandas related functionality,
    such as toPandas, createDataFrame from pandas DataFrame, and so on."""

    def __init__(
        self,
        pyspark_version: str = "3.0",
        required_dependency_name: str = "pandas",
        required_dependency_version: str = "0.23.2",
    ):
        super().__init__(
            transformer_id="PY24-30-001",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
        )


class ToPandasUsageTransformer(StatementLineCommentWriter):
    """In Spark 3.0, PySpark requires a pandas version of 0.23.2 or higher to use pandas related functionality,
    such as toPandas, createDataFrame from pandas DataFrame, and so on."""

    def __init__(
        self,
        pyspark_version: str = "3.0",
        required_dependency_name: str = "pandas",
        required_dependency_version: str = "0.23.2",
    ):
        super().__init__(
            transformer_id="PY24-30-002",
            comment=f"PySpark {pyspark_version} requires a {required_dependency_name} version of \
{required_dependency_version} or higher to use toPandas()",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if toPandas is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("toPandas")))):
            self.match_found = True


class PandasUdfUsageTransformer(RequiredDependencyVersionCommentWriter):
    """In Spark 3.0, PySpark requires a PyArrow version of 0.12.1 or higher to use PyArrow related functionality, such
    as pandas_udf, toPandas and createDataFrame with “spark.sql.execution.arrow.enabled=true”, etc.
    """

    def __init__(
        self,
        pyspark_version: str = "3.0",
        required_dependency_name: str = "PyArrow",
        required_dependency_version: str = "0.12.1",
    ):
        super().__init__(
            transformer_id="PY24-30-003",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
            import_name="pandas_udf",
        )


class PyArrowEnabledCommentWriter(StatementLineCommentWriter):
    """In Spark 3.0, PySpark requires a PyArrow version of 0.12.1 or higher to use PyArrow related functionality, such
    as pandas_udf, toPandas and createDataFrame with “spark.sql.execution.arrow.enabled=true”, etc.
    """

    def __init__(
        self,
        transformer_id: str = "PY24-30-004",
        pyspark_version: str = "3.0",
        required_dependency_name: str = "PyArrow",
        required_dependency_version: str = "0.12.1",
    ):
        super().__init__(
            transformer_id=transformer_id,
            comment=f"PySpark {pyspark_version} requires {required_dependency_name} version \
{required_dependency_version} or higher when spark.sql.execution.arrow.enabled is set to true",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Attribute(attr=m.Name("set")),
                    m.Attribute(attr=m.Name("config")),
                ),
                args=[
                    m.Arg(
                        value=one_of_matching_strings(
                            "spark.sql.execution.arrow.enabled",
                            "spark.sql.execution.arrow.pyspark.enabled",
                        )
                    ),
                    m.Arg(value=one_of_matching_strings("true")),
                ],
            ),
        ):
            self.match_found = True


class PandasConvertToArrowArraySafelyCommentWriter(PyArrowEnabledCommentWriter):
    """In PySpark, when PyArrow optimization is enabled, Arrow can perform safe type conversion when converting
    pandas.Series to an Arrow array during serialization. Arrow raises errors when detecting unsafe type conversions
    like overflow. You enable it by setting spark.sql.execution.pandas.convertToArrowArraySafely to true.
    The default setting is false.
    """

    def __init__(self):
        super().__init__(transformer_id="PY24-30-005")

    @property
    def comment(self):
        return "Consider setting spark.sql.execution.pandas.convertToArrowArraySafely to true to raise errors in case \
of Integer overflow or Floating point truncation, instead of silent allows."

    @comment.setter
    def comment(self, value):
        self._comment = value


class CreateDataFrameVerifySchemaCommentWriter(StatementLineCommentWriter):
    """In Spark 3.0, createDataFrame(..., verifySchema=True) validates LongType as well in PySpark. Previously, LongType
    was not verified and resulted in None in case the value overflows. To restore this behavior, verifySchema can be set
    to `False` to disable the validation.
    """

    def __init__(
        self,
        pyspark_version: str = "3.0",
    ):
        super().__init__(
            transformer_id="PY24-30-006",
            comment=f"Setting verifySchema to True validates LongType as well in PySpark {pyspark_version}. \
Previously, LongType was not verified and resulted in None in case the value overflows.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if createDataFrame(..., verifySchema=True) is being called"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.Name("createDataFrame")),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name("verifySchema"), value=m.Name("True")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class RowFieldNamesNotSortedCommentWriter(StatementLineCommentWriter):
    """As of Spark 3.0, Row field names are no longer sorted alphabetically when constructing with named arguments for
    Python versions 3.6 and above, and the order of fields will match that as entered. For Python versions less than
    3.6, the field names will be sorted alphabetically as the only option.
    """

    def __init__(
        self,
        pyspark_version: str = "3.0",
    ):
        super().__init__(
            transformer_id="PY24-30-007",
            comment=f"Sorting Row fields by name alphabetically since as of Spark {pyspark_version}, they are no \
longer when constructed with named arguments.",
        )

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        """Check if Row(...) is being called with named arguments and sort Dataframe Row fields by name alphabetically
        when constructed with named arguments for backwards compatibility"""
        if m.matches(
            original_node,
            m.Call(
                func=m.Name("Row"),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name()),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True
            row_fields = sorted(
                updated_node.args,
                key=lambda arg: arg.keyword.value,
            )
            return updated_node.with_changes(args=row_fields)
        else:
            return updated_node


class MlParamMixinsSetterCommentWriter(StatementLineCommentWriter):
    """In Spark 3.0, pyspark.ml.param.shared.Has* mixins do not provide any set*(self, value) setter methods anymore,
    use the respective self.set(self.*, value) instead. See SPARK-29093 for details.
    """

    def __init__(
        self,
        pyspark_version: str = "3.0",
    ):
        super().__init__(
            transformer_id="PY24-30-008",
            comment=f"In Spark {pyspark_version}, pyspark.ml.param.shared.Has* mixins do not provide any \
set*(self, value) setter methods anymore, use the respective self.set(self.*, value) instead.",
        )

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        """Check if pyspark.ml.param.shared is being used in a from import statement"""
        if m.matches(
            node,
            m.ImportFrom(
                module=m.Attribute(
                    value=m.Attribute(
                        value=m.Attribute(
                            value=m.Name("pyspark"),
                            attr=m.Name("ml"),
                        ),
                        attr=m.Name("param"),
                    ),
                    attr=m.Name("shared"),
                ),
            ),
        ):
            self.match_found = True


def pyspark_24_to_30_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 2.4 to 3.0 migration guide"""
    return [
        RequiredPandasVersionCommentWriter(),
        ToPandasUsageTransformer(),
        PandasUdfUsageTransformer(),
        PyArrowEnabledCommentWriter(),
        PandasConvertToArrowArraySafelyCommentWriter(),
        CreateDataFrameVerifySchemaCommentWriter(),
        RowFieldNamesNotSortedCommentWriter(),
        MlParamMixinsSetterCommentWriter(),
    ]
