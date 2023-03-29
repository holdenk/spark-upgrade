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

from pysparkler import (
    BaseMatcherDecoratableTransformer,
    BaseTransformer,
    add_comment_to_end_of_a_simple_statement_line,
)

# Migration rules for PySpark 2.4 to 3.0
# https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_2.4_to_3.0.html


class RequiredDependencyVersionCommentWriter(BaseTransformer):
    """Base class for adding comments to the import statements of required dependencies version of PySpark"""

    def __init__(
        self,
        transformer_id: str,
        pyspark_version: str,
        required_dependency_name: str,
        required_dependency_version: str,
    ):
        super().__init__(transformer_id)
        self.pyspark_version = pyspark_version
        self.required_dependency_version = required_dependency_version
        self.required_dependency_name = required_dependency_name

    def leave_SimpleStatementLine(self, original_node, updated_node):
        """Add a comment to the dependency import statement"""
        match_found = False
        for child in original_node.body:
            if isinstance(child, cst.Import):
                for alias in child.names:
                    if alias.name.value == self.required_dependency_name:
                        match_found = True
                        break
            elif isinstance(child, cst.ImportFrom):
                if (
                    child.module is not None
                    and child.module.value == self.required_dependency_name
                ):
                    match_found = True
                    break

        if match_found:
            return add_comment_to_end_of_a_simple_statement_line(
                updated_node,
                self.transformer_id,
                f"# {self.transformer_id}: PySpark {self.pyspark_version} requires {self.required_dependency_name} version {self.required_dependency_version} or higher",
            )
        else:
            return updated_node


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


class ToPandasUsageTransformer(BaseMatcherDecoratableTransformer):
    """In Spark 3.0, PySpark requires a pandas version of 0.23.2 or higher to use pandas related functionality,
    such as toPandas, createDataFrame from pandas DataFrame, and so on."""

    def __init__(
        self,
        pyspark_version: str = "3.0",
        required_dependency_name: str = "pandas",
        required_dependency_version: str = "0.23.2",
    ):
        super().__init__(transformer_id="PY24-30-002")
        self.pyspark_version = pyspark_version
        self.required_dependency_version = required_dependency_version
        self.required_dependency_name = required_dependency_name
        self.match_found = False

    def visit_Attribute(self, node: cst.Attribute) -> None:
        """Check if toPandas is being used"""
        if m.matches(node, m.Attribute(attr=m.Name("toPandas"))):
            self.match_found = True

    def leave_SimpleStatementLine(self, original_node, updated_node):
        """Add a comment where to Pandas is being used"""
        if self.match_found:
            self.match_found = False
            return add_comment_to_end_of_a_simple_statement_line(
                updated_node,
                self.transformer_id,
                f"# {self.transformer_id}: PySpark {self.pyspark_version} requires a {self.required_dependency_name} version of {self.required_dependency_version} or higher to use toPandas()",
            )
        else:
            return original_node
