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

# Migration rules for PySpark 3.5 to 4.0
# https://spark.apache.org/docs/4.0.0/api/python/migration_guide/pyspark_upgrade.html


class RequiredPandasVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In Spark 4.0, the minimum supported version for Pandas has been raised from 1.0.5 to 2.0.0 in PySpark."""

    def __init__(
        self,
        pyspark_version: str = "4.0",
        required_dependency_name: str = "pandas",
        required_dependency_version: str = "2.0.0",
    ):
        super().__init__(
            transformer_id="PY35-40-001",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
        )


class RequiredNumpyVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In Spark 4.0, the minimum supported version for Numpy has been raised from 1.15 to 1.21 in PySpark."""

    def __init__(
        self,
        pyspark_version: str = "4.0",
        required_dependency_name: str = "numpy",
        required_dependency_version: str = "1.21",
    ):
        super().__init__(
            transformer_id="PY35-40-002",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
        )


class RequiredPyArrowVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In Spark 4.0, the minimum supported version for PyArrow has been raised from 4.0.0 to 11.0.0 in PySpark."""

    def __init__(
        self,
        pyspark_version: str = "4.0",
        required_dependency_name: str = "PyArrow",
        required_dependency_version: str = "11.0.0",
    ):
        super().__init__(
            transformer_id="PY35-40-003",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
            import_name="pandas_udf",
        )


class PandasIteritemsRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DataFrame.iteritems and Series.iteritems have been removed from pandas API on Spark, use
    DataFrame.items and Series.items instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-004",
            comment=f"As of PySpark {pyspark_version}, DataFrame.iteritems and Series.iteritems have been removed \
from pandas API on Spark, use DataFrame.items and Series.items instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed iteritems method is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("iteritems")))):
            self.match_found = True


class PandasToKoalasRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DataFrame.to_koalas and DataFrame.to_pandas_on_spark have been removed from PySpark, use
    DataFrame.pandas_api instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-005",
            comment=f"As of PySpark {pyspark_version}, DataFrame.to_koalas and DataFrame.to_pandas_on_spark have \
been removed, use DataFrame.pandas_api instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed to_koalas / to_pandas_on_spark methods are being called"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(
                    attr=m.OneOf(
                        m.Name("to_koalas"),
                        m.Name("to_pandas_on_spark"),
                    )
                )
            ),
        ):
            self.match_found = True


class PandasIndexClassesRemoved(StatementLineCommentWriter):
    """In Spark 4.0, Int64Index and Float64Index have been removed from pandas API on Spark, Index should be used
    directly.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-006",
            comment=f"As of PySpark {pyspark_version}, Int64Index and Float64Index have been removed from pandas API \
on Spark, use Index directly instead.",
        )

    def visit_Name(self, node: cst.Name) -> None:
        """Check if the removed Int64Index / Float64Index classes are being referenced"""
        if m.matches(node, m.OneOf(m.Name("Int64Index"), m.Name("Float64Index"))):
            self.match_found = True


class AnsiModeEnabledByDefault(StatementLineCommentWriter):
    """In Spark 4.0, ANSI SQL mode is enabled by default (spark.sql.ansi.enabled). Pandas API on Spark will raise an
    exception when the underlying Spark is working with ANSI mode enabled, as it does not work properly with ANSI mode.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-007",
            comment=f"As of PySpark {pyspark_version}, ANSI mode is enabled by default and pandas API on Spark raises \
an exception under ANSI mode. Disable it via spark.sql.ansi.enabled=false, or set the pandas-on-spark option \
compute.fail_on_ansi_mode=False to force it to work.",
        )

    def visit_Import(self, node: cst.Import) -> None:
        """Check if pyspark.pandas is being imported"""
        if m.matches(
            node,
            m.Import(
                names=[
                    m.ImportAlias(
                        name=m.Attribute(value=m.Name("pyspark"), attr=m.Name("pandas"))
                    )
                ]
            ),
        ):
            self.match_found = True

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        """Check if pyspark.pandas is being imported via a from import statement"""
        if m.matches(
            node,
            m.ImportFrom(
                module=m.Attribute(value=m.Name("pyspark"), attr=m.Name("pandas"))
            ),
        ):
            self.match_found = True


class SqlFunctionsStarImport(StatementLineCommentWriter):
    """In Spark 4.0, items other than functions (e.g. DataFrame, Column, StructType) have been removed from the
    wildcard import `from pyspark.sql.functions import *`.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-008",
            comment=f"As of PySpark {pyspark_version}, items other than functions (e.g. DataFrame, Column, StructType) \
are no longer exported from `from pyspark.sql.functions import *`. Import them from their proper modules instead, \
e.g. `from pyspark.sql import DataFrame, Column` and `from pyspark.sql.types import StructType`.",
        )

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        """Check if a wildcard import is being done from pyspark.sql.functions"""
        if m.matches(
            node,
            m.ImportFrom(
                module=m.Attribute(
                    value=m.Attribute(value=m.Name("pyspark"), attr=m.Name("sql")),
                    attr=m.Name("functions"),
                ),
                names=m.ImportStar(),
            ),
        ):
            self.match_found = True


class FactorizeNaSentinelRenamed(StatementLineCommentWriter):
    """In Spark 4.0, the na_sentinel parameter from Index.factorize and Series.factorize has been removed from pandas
    API on Spark, use use_na_sentinel instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-009",
            comment=f"As of PySpark {pyspark_version}, the na_sentinel parameter of factorize has been removed from \
pandas API on Spark, use use_na_sentinel instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if factorize is being called with the removed na_sentinel keyword argument"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.Name("factorize")),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name("na_sentinel")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


def pyspark_35_to_40_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 3.5 to 4.0 migration guide"""
    return [
        RequiredPandasVersionCommentWriter(),
        RequiredNumpyVersionCommentWriter(),
        RequiredPyArrowVersionCommentWriter(),
        PandasIteritemsRemoved(),
        PandasToKoalasRemoved(),
        PandasIndexClassesRemoved(),
        AnsiModeEnabledByDefault(),
        SqlFunctionsStarImport(),
        FactorizeNaSentinelRenamed(),
    ]
