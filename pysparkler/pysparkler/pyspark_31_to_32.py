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

from pysparkler.base import StatementLineCommentWriter


class SqlMlMethodsRaiseTypeErrorCommentWriter(StatementLineCommentWriter):
    """In Spark 3.2, the PySpark methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError
    when are applied to a param of inappropriate type.
    """

    def __init__(
        self,
        pyspark_version: str = "3.2",
    ):
        super().__init__(
            transformer_id="PY31-32-001",
            comment=f"As of PySpark {pyspark_version}, the methods from sql, ml, spark_on_pandas modules raise the \
TypeError instead of ValueError when are applied to a param of inappropriate type.",
        )
        self._has_sql_or_ml_import = False

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        """Check if pyspark.sql.* or pyspark.ml.* is being used in a from import statement"""
        if m.matches(
            node,
            m.ImportFrom(
                module=m.Attribute(
                    value=m.OneOf(
                        m.Attribute(
                            value=m.Name("pyspark"),
                            attr=m.Name("sql"),
                        ),
                        m.Attribute(
                            value=m.Name("pyspark"),
                            attr=m.Name("ml"),
                        ),
                    ),
                ),
            ),
        ):
            self._has_sql_or_ml_import = True

    def visit_Import(self, node: cst.Import) -> None:
        """Check if pyspark.sql.* or pyspark.ml.* is being used in an import statement"""
        if m.matches(
            node,
            m.Import(
                names=[
                    m.OneOf(
                        m.ImportAlias(
                            name=m.Attribute(
                                value=m.Attribute(
                                    value=m.Name("pyspark"),
                                    attr=m.Name("sql"),
                                )
                            ),
                        ),
                        m.ImportAlias(
                            name=m.Attribute(
                                value=m.Attribute(
                                    value=m.Name("pyspark"),
                                    attr=m.Name("ml"),
                                )
                            ),
                        ),
                    ),
                    m.ZeroOrMore(),
                ]
            ),
        ):
            self._has_sql_or_ml_import = True

    def visit_ExceptHandler(self, node: cst.ExceptHandler) -> None:
        """Check if the except handler is catching the ValueError"""
        if m.matches(
            node,
            m.ExceptHandler(
                type=m.Name("ValueError"),
            ),
        ):
            if self._has_sql_or_ml_import:
                self.match_found = True


def pyspark_31_to_32_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 3.1 to 3.2 migration guide"""
    return [SqlMlMethodsRaiseTypeErrorCommentWriter()]
