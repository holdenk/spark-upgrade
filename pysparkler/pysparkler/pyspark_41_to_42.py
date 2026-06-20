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

# Migration rules for PySpark 4.1 to 4.2
# https://spark.apache.org/docs/4.2.0-preview4/api/python/migration_guide/pyspark_upgrade.html
# NOTE: Spark 4.2 is still a preview release; these rules track the preview migration guide and
# may change before the final 4.2.0 release.


class RequiredPyArrowVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In Spark 4.2, the minimum supported version for PyArrow has been raised from 15.0.0 to 18.0.0 in PySpark."""

    def __init__(
        self,
        pyspark_version: str = "4.2",
        required_dependency_name: str = "PyArrow",
        required_dependency_version: str = "18.0.0",
    ):
        super().__init__(
            transformer_id="PY41-42-001",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
            import_name="pandas_udf",
        )


class ArrowDataExchangeEnabledByDefault(StatementLineCommentWriter):
    """In Spark 4.2, columnar data exchange between PySpark and the JVM uses Apache Arrow by default. The
    configuration spark.sql.execution.arrow.pyspark.enabled now defaults to true.
    """

    def __init__(
        self,
        pyspark_version: str = "4.2",
    ):
        super().__init__(
            transformer_id="PY41-42-002",
            comment=f"As of PySpark {pyspark_version}, columnar data exchange between PySpark and the JVM uses Apache \
Arrow by default (spark.sql.execution.arrow.pyspark.enabled defaults to true). To restore the legacy row-based \
exchange, set it to false.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if toPandas is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("toPandas")))):
            self.match_found = True


class ArrowOptimizedPythonUdfByDefault(StatementLineCommentWriter):
    """In Spark 4.2, regular Python UDFs are Arrow-optimized by default. The configuration
    spark.sql.execution.pythonUDF.arrow.enabled now defaults to true.
    """

    def __init__(
        self,
        pyspark_version: str = "4.2",
    ):
        super().__init__(
            transformer_id="PY41-42-003",
            comment=f"As of PySpark {pyspark_version}, regular Python UDFs are Arrow-optimized by default \
(spark.sql.execution.pythonUDF.arrow.enabled defaults to true). To restore the legacy behavior, set it to false.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if a regular Python UDF is being created via udf(...)"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Name("udf"),
                    m.Attribute(attr=m.Name("udf")),
                )
            ),
        ):
            self.match_found = True


class ArrowOptimizedPythonUdtfByDefault(StatementLineCommentWriter):
    """In Spark 4.2, regular Python UDTFs are Arrow-optimized by default. The configuration
    spark.sql.execution.pythonUDTF.arrow.enabled now defaults to true.
    """

    def __init__(
        self,
        pyspark_version: str = "4.2",
    ):
        super().__init__(
            transformer_id="PY41-42-004",
            comment=f"As of PySpark {pyspark_version}, regular Python UDTFs are Arrow-optimized by default \
(spark.sql.execution.pythonUDTF.arrow.enabled defaults to true). To restore the legacy behavior, set it to false.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if a regular Python UDTF is being created via udtf(...)"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Name("udtf"),
                    m.Attribute(attr=m.Name("udtf")),
                )
            ),
        ):
            self.match_found = True


def pyspark_41_to_42_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 4.1 to 4.2 migration guide"""
    return [
        RequiredPyArrowVersionCommentWriter(),
        ArrowDataExchangeEnabledByDefault(),
        ArrowOptimizedPythonUdfByDefault(),
        ArrowOptimizedPythonUdtfByDefault(),
    ]
