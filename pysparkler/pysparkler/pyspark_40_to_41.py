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

# Migration rules for PySpark 4.0 to 4.1
# https://spark.apache.org/docs/4.1.0/api/python/migration_guide/pyspark_upgrade.html


class RequiredPyArrowVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In Spark 4.1, the minimum supported version for PyArrow has been raised from 11.0.0 to 15.0.0 in PySpark."""

    def __init__(
        self,
        pyspark_version: str = "4.1",
        required_dependency_name: str = "PyArrow",
        required_dependency_version: str = "15.0.0",
    ):
        super().__init__(
            transformer_id="PY40-41-001",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
            import_name="pandas_udf",
        )


class RequiredPandasVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In Spark 4.1, the minimum supported version for Pandas has been raised from 2.0.0 to 2.2.0 in PySpark."""

    def __init__(
        self,
        pyspark_version: str = "4.1",
        required_dependency_name: str = "pandas",
        required_dependency_version: str = "2.2.0",
    ):
        super().__init__(
            transformer_id="PY40-41-002",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
        )


class Python39SupportDropped(StatementLineCommentWriter):
    """In Spark 4.1, Python 3.9 support was dropped in PySpark."""

    def __init__(
        self,
        pyspark_version: str = "4.1",
    ):
        super().__init__(
            transformer_id="PY40-41-003",
            comment=f"As of PySpark {pyspark_version}, Python 3.9 support has been dropped, Python 3.10 or higher is \
required.",
        )

    def visit_Import(self, node: cst.Import) -> None:
        """Check if the top-level pyspark package is being imported, even alongside other modules"""
        if m.matches(
            node,
            m.Import(
                names=[
                    m.ZeroOrMore(),
                    m.ImportAlias(name=m.Name("pyspark")),
                    m.ZeroOrMore(),
                ]
            ),
        ):
            self.match_found = True


class BinaryTypeMapsToBytes(StatementLineCommentWriter):
    """In Spark 4.1, the data type BinaryType is by default mapped consistently to Python bytes in PySpark. Previously
    it was mapped to bytearray across all cases.
    """

    def __init__(
        self,
        pyspark_version: str = "4.1",
    ):
        super().__init__(
            transformer_id="PY40-41-004",
            comment=f"As of PySpark {pyspark_version}, BinaryType is mapped to Python bytes instead of bytearray. To \
restore the previous behavior, set spark.sql.execution.pyspark.binaryAsBytes to false.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if BinaryType is being constructed (directly or via a module attribute)"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Name("BinaryType"),
                    m.Attribute(attr=m.Name("BinaryType")),
                )
            ),
        ):
            self.match_found = True


class ConvertToArrowArraySafelyEnabledByDefault(StatementLineCommentWriter):
    """In Spark 4.1, the spark.sql.execution.pandas.convertToArrowArraySafely configuration is enabled by default.
    When enabled, PyArrow raises errors for unsafe conversions such as integer overflows, floating point truncation,
    and loss of precision.
    """

    def __init__(
        self,
        pyspark_version: str = "4.1",
    ):
        super().__init__(
            transformer_id="PY40-41-005",
            comment=f"As of PySpark {pyspark_version}, spark.sql.execution.pandas.convertToArrowArraySafely is enabled \
by default, so PyArrow raises errors on unsafe conversions (integer overflow, float truncation, loss of precision). \
To restore the previous behavior, set it to false.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if pandas_udf is being used (as a decorator factory or function)"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Name("pandas_udf"),
                    m.Attribute(attr=m.Name("pandas_udf")),
                )
            ),
        ):
            self.match_found = True


def pyspark_40_to_41_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 4.0 to 4.1 migration guide"""
    return [
        RequiredPyArrowVersionCommentWriter(),
        RequiredPandasVersionCommentWriter(),
        Python39SupportDropped(),
        BinaryTypeMapsToBytes(),
        ConvertToArrowArraySafelyEnabledByDefault(),
    ]
