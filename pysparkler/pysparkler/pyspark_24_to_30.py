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

from pysparkler import BaseTransformer, add_comment_to_end_of_a_simple_statement_line

# Migration rules for PySpark 2.4 to 3.0
# https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_2.4_to_3.0.html


class StatementLineCommentWriter(BaseTransformer):
    """Base class for adding comments to the end of the statement line when a matching condition is found"""

    def __init__(
        self,
        transformer_id: str,
        comment: str,
    ):
        super().__init__(transformer_id)
        self._comment = comment
        self.match_found = False

    @property
    def comment(self):
        return self._comment

    def leave_SimpleStatementLine(self, original_node, updated_node):
        """Add a comment where to Pandas is being used"""
        if self.match_found:
            self.match_found = False
            return add_comment_to_end_of_a_simple_statement_line(
                updated_node,
                self.transformer_id,
                f"# {self.transformer_id}: {self.comment}",
            )
        else:
            return original_node


class RequiredDependencyVersionCommentWriter(StatementLineCommentWriter):
    """Base class for adding comments to the import statements of required dependencies version of PySpark"""

    def __init__(
        self,
        transformer_id: str,
        pyspark_version: str,
        required_dependency_name: str,
        required_dependency_version: str,
        import_name: str | None = None,
    ):
        super().__init__(
            transformer_id=transformer_id,
            comment=f"PySpark {pyspark_version} requires {required_dependency_name} version {required_dependency_version} or higher",
        )
        self.required_dependency_name = required_dependency_name
        self._import_name = import_name

    @property
    def import_name(self):
        return (
            self.required_dependency_name
            if self._import_name is None
            else self._import_name
        )

    @property
    def comment(self):
        if self._import_name is None:
            return super().comment
        else:
            return f"{super().comment} to use {self._import_name}"

    def visit_Import(self, node: cst.Import) -> None:
        """Check if pandas_udf is being used in an import statement"""
        if m.matches(
            node,
            m.Import(
                names=[m.ImportAlias(name=m.Attribute(attr=m.Name(self.import_name)))]
            ),
        ):
            self.match_found = True

    def visit_ImportAlias(self, node: cst.ImportAlias) -> None:
        """Check if pandas_udf is being used in a from import statement"""
        if m.matches(node, m.ImportAlias(name=m.Name(self.import_name))):
            self.match_found = True

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        """Check if pandas_udf is being used in a from import statement"""
        if m.matches(node, m.ImportFrom(module=m.Name(self.import_name))):
            self.match_found = True


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
            comment=f"PySpark {pyspark_version} requires a {required_dependency_name} version of {required_dependency_version} or higher to use toPandas()",
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
