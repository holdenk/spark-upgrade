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
)


class DataframeDropAxisIndexByDefault(StatementLineCommentWriter):
    """In Spark 3.3, the drop method of pandas API on Spark DataFrame supports dropping rows by index, and sets dropping
    by index instead of column by default.
    """

    def __init__(
        self,
        pyspark_version: str = "3.3",
    ):
        super().__init__(
            transformer_id="PY32-33-001",
            comment=f"Explicitly setting axis to 1 to drop by column, since as of PySpark {pyspark_version} the drop \
method of pandas API on Spark DataFrame sets drop by index as default, instead of drop by column.",
        )
        self.inside_drop_call = False

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        """Specify the axis argument to 1 if it is not specified to maintain the behavior of dropping columns"""
        if m.matches(
            original_node,
            m.Call(
                func=m.Attribute(
                    attr=m.Name("drop"),
                ),
                args=[
                    m.OneOf(
                        m.Arg(keyword=m.Name("labels")),
                        m.Arg(keyword=None),
                    )
                ],
            ),
        ):
            self.match_found = True
            return updated_node.with_changes(
                args=[
                    *updated_node.args,
                    cst.Arg(
                        keyword=cst.Name("axis"),
                        value=cst.Integer("1"),
                        equal=cst.AssignEqual(
                            whitespace_before=cst.SimpleWhitespace(""),
                            whitespace_after=cst.SimpleWhitespace(""),
                        ),
                    ),
                ]
            )
        else:
            return updated_node


class RequiredPandasVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In Spark 3.3, PySpark upgrades Pandas version, the new minimum required version changes from 0.23.2 to 1.0.5."""

    def __init__(
        self,
        pyspark_version: str = "3.3",
        required_dependency_name: str = "pandas",
        required_dependency_version: str = "1.0.5",
    ):
        super().__init__(
            transformer_id="PY32-33-002",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
        )


class SQLDataTypesReprReturnsObjectCommentWriter(StatementLineCommentWriter):
    """In Spark 3.3, the repr return values of SQL DataTypes have been changed to yield an object with the same value
    when passed to eval.
    """

    def __init__(
        self,
        pyspark_version: str = "3.3",
    ):
        super().__init__(
            transformer_id="PY32-33-003",
            comment=f"As of PySpark {pyspark_version}, the repr return values of SQL DataTypes have been changed to \
yield an object with the same value when passed to eval.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the repr method of SQL DataTypes is called"""
        if m.matches(
            node,
            m.Call(func=m.Name("repr")),
        ):
            self.match_found = True


def pyspark_32_to_33_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 3.2 to 3.3 migration guide"""
    return [
        DataframeDropAxisIndexByDefault(),
        RequiredPandasVersionCommentWriter(),
        SQLDataTypesReprReturnsObjectCommentWriter(),
    ]
