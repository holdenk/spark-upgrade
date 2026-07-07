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
    PySparkImportCommentWriter,
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

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed Int64Index / Float64Index classes are being constructed"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Name("Int64Index"),
                    m.Name("Float64Index"),
                    m.Attribute(attr=m.Name("Int64Index")),
                    m.Attribute(attr=m.Name("Float64Index")),
                )
            ),
        ):
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

    @staticmethod
    def _is_pyspark_pandas(node: cst.BaseExpression | None) -> bool:
        """Return True for a dotted path that is ``pyspark.pandas`` or a submodule of it."""
        parts: list[str] = []
        while isinstance(node, cst.Attribute):
            parts.append(node.attr.value)
            node = node.value
        if isinstance(node, cst.Name):
            parts.append(node.value)
        parts.reverse()
        return parts[:2] == ["pyspark", "pandas"]

    def visit_Import(self, node: cst.Import) -> None:
        """Check if pyspark.pandas (or a submodule) is imported, possibly alongside other modules"""
        if any(self._is_pyspark_pandas(alias.name) for alias in node.names):
            self.match_found = True

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        """Check if pyspark.pandas is imported via a from-import statement"""
        # Covers ``from pyspark.pandas[.submodule] import x`` as well as ``from pyspark import pandas``.
        if node.module is None:
            return
        if self._is_pyspark_pandas(node.module):
            self.match_found = True
        elif (
            m.matches(node.module, m.Name("pyspark"))
            and not isinstance(node.names, cst.ImportStar)
            and any(m.matches(alias.name, m.Name("pandas")) for alias in node.names)
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


class Python38SupportDropped(PySparkImportCommentWriter):
    """In Spark 4.0, Python 3.8 support was dropped in PySpark."""

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-010",
            comment=f"As of PySpark {pyspark_version}, Python 3.8 support has been dropped, Python 3.9 or higher is \
required.",
        )


class PandasAppendRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DataFrame.append and Series.append have been removed from pandas API on Spark, use ps.concat
    instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-011",
            comment=f"As of PySpark {pyspark_version}, if this is a pandas-on-Spark DataFrame or Series then \
.append has been removed, use ps.concat instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed append method is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("append")))):
            self.match_found = True


class PandasMadRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DataFrame.mad and Series.mad have been removed from pandas API on Spark."""

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-012",
            comment=f"As of PySpark {pyspark_version}, if this is a pandas-on-Spark DataFrame or Series then \
.mad has been removed; compute the mean absolute deviation manually instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed mad method is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("mad")))):
            self.match_found = True


class PandasToSparkIoRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DataFrame.to_spark_io has been removed from pandas API on Spark, use DataFrame.spark.to_spark_io
    instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-013",
            comment=f"As of PySpark {pyspark_version}, DataFrame.to_spark_io has been removed from pandas API on \
Spark, use DataFrame.spark.to_spark_io instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed to_spark_io method is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("to_spark_io")))):
            self.match_found = True


class PandasGetDtypeCountsRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DataFrame.get_dtype_counts has been removed from pandas API on Spark, use
    DataFrame.dtypes.value_counts() instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-014",
            comment=f"As of PySpark {pyspark_version}, DataFrame.get_dtype_counts has been removed from pandas API \
on Spark, use DataFrame.dtypes.value_counts() instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed get_dtype_counts method is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("get_dtype_counts")))):
            self.match_found = True


class PandasKoalasAccessorRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DataFrame.koalas has been removed from pandas API on Spark, use DataFrame.pandas_on_spark
    instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-015",
            comment=f"As of PySpark {pyspark_version}, the .koalas accessor has been removed from pandas API on \
Spark, use .pandas_on_spark instead.",
        )

    def visit_Attribute(self, node: cst.Attribute) -> None:
        """Check if the removed koalas accessor is being used"""
        if m.matches(node, m.Attribute(attr=m.Name("koalas"))):
            self.match_found = True


class PandasIndexApisRemoved(StatementLineCommentWriter):
    """In Spark 4.0, Index.asi8, Index.is_type_compatible and Index.is_all_dates have been removed from pandas API on
    Spark.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-016",
            comment=f"As of PySpark {pyspark_version}, Index.asi8 (use Index.astype instead), \
Index.is_type_compatible (use Index.isin instead) and Index.is_all_dates have been removed from pandas API on Spark.",
        )

    def visit_Attribute(self, node: cst.Attribute) -> None:
        """Check if the removed asi8 / is_all_dates properties are being accessed"""
        if m.matches(
            node, m.Attribute(attr=m.OneOf(m.Name("asi8"), m.Name("is_all_dates")))
        ):
            self.match_found = True

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed is_type_compatible method is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("is_type_compatible")))):
            self.match_found = True


class PandasIsMonotonicRemoved(StatementLineCommentWriter):
    """In Spark 4.0, Series.is_monotonic and Index.is_monotonic have been removed from pandas API on Spark, use
    is_monotonic_increasing instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-017",
            comment=f"As of PySpark {pyspark_version}, Series.is_monotonic and Index.is_monotonic have been removed \
from pandas API on Spark, use is_monotonic_increasing instead.",
        )

    def visit_Attribute(self, node: cst.Attribute) -> None:
        """Check if the removed is_monotonic property is being accessed"""
        if m.matches(node, m.Attribute(attr=m.Name("is_monotonic"))):
            self.match_found = True


class PandasWeekOfYearRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DatetimeIndex.week/weekofyear and Series.dt.week/weekofyear have been removed from pandas API on
    Spark, use isocalendar().week instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-018",
            comment=f"As of PySpark {pyspark_version}, if this is a pandas-on-Spark DatetimeIndex or Series.dt then \
.week and .weekofyear have been removed, use .isocalendar().week instead.",
        )

    def visit_Attribute(self, node: cst.Attribute) -> None:
        """Check if the removed week / weekofyear properties are being accessed"""
        if m.matches(
            node, m.Attribute(attr=m.OneOf(m.Name("week"), m.Name("weekofyear")))
        ):
            # Skip the already-migrated ``...isocalendar().week`` form.
            if m.matches(
                node.value, m.Call(func=m.Attribute(attr=m.Name("isocalendar")))
            ):
                return
            self.match_found = True


class PandasGroupByBackfillPadRemoved(StatementLineCommentWriter):
    """In Spark 4.0, DataFrameGroupBy.backfill and DataFrameGroupBy.pad have been removed from pandas API on Spark, use
    bfill and ffill instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-019",
            comment=f"As of PySpark {pyspark_version}, if this is a pandas-on-Spark GroupBy then .backfill (use \
.bfill instead) and .pad (use .ffill instead) have been removed.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed backfill / pad GroupBy methods are being called"""
        if m.matches(
            node,
            m.Call(func=m.Attribute(attr=m.OneOf(m.Name("backfill"), m.Name("pad")))),
        ):
            self.match_found = True


class PandasCategoricalInplaceRemoved(StatementLineCommentWriter):
    """In Spark 4.0, the inplace parameter of the Categorical / CategoricalIndex category methods has been removed from
    pandas API on Spark.
    """

    _category_methods = (
        "add_categories",
        "remove_categories",
        "remove_unused_categories",
        "set_categories",
        "rename_categories",
        "reorder_categories",
        "as_ordered",
        "as_unordered",
    )

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-020",
            comment=f"As of PySpark {pyspark_version}, the inplace parameter of the Categorical and CategoricalIndex \
category methods has been removed from pandas API on Spark; assign the returned value instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if a category method is called with the removed inplace keyword argument"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(
                    attr=m.OneOf(*[m.Name(name) for name in self._category_methods])
                ),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name("inplace")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class PandasDateRangeClosedRemoved(StatementLineCommentWriter):
    """In Spark 4.0, the closed parameter from ps.date_range has been removed from pandas API on Spark."""

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-021",
            comment=f"As of PySpark {pyspark_version}, the closed parameter of ps.date_range has been removed from \
pandas API on Spark, use the inclusive parameter instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if date_range is called with the removed closed keyword argument"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Name("date_range"), m.Attribute(attr=m.Name("date_range"))
                ),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name("closed")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class PandasBetweenTimeInclusiveRemoved(StatementLineCommentWriter):
    """In Spark 4.0, the include_start and include_end parameters from DataFrame.between_time and Series.between_time
    have been removed from pandas API on Spark, use inclusive instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-022",
            comment=f"As of PySpark {pyspark_version}, the include_start and include_end parameters of between_time \
have been removed from pandas API on Spark, use the inclusive parameter instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if between_time is called with the removed include_start / include_end keyword arguments"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.Name("between_time")),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(
                        keyword=m.OneOf(m.Name("include_start"), m.Name("include_end"))
                    ),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class PandasSeriesBetweenBooleanInclusiveRemoved(StatementLineCommentWriter):
    """In Spark 4.0, passing True or False to the inclusive parameter of Series.between has been removed from pandas
    API on Spark, use "both" or "neither" instead respectively.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-023",
            comment=f'As of PySpark {pyspark_version}, passing True or False to the inclusive parameter of \
Series.between has been removed from pandas API on Spark, use "both" or "neither" instead respectively.',
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if between is called with a boolean inclusive argument (keyword or third positional)"""
        boolean = m.OneOf(m.Name("True"), m.Name("False"))
        keyword_form = m.Call(
            func=m.Attribute(attr=m.Name("between")),
            args=[
                m.ZeroOrMore(),
                m.Arg(keyword=m.Name("inclusive"), value=boolean),
                m.ZeroOrMore(),
            ],
        )
        # Spark 3.5 also accepted ``inclusive`` as the third positional argument,
        # e.g. ``series.between(1, 5, False)``.
        positional_form = m.Call(
            func=m.Attribute(attr=m.Name("between")),
            args=[
                m.Arg(keyword=None),
                m.Arg(keyword=None),
                m.Arg(keyword=None, value=boolean),
                m.ZeroOrMore(),
            ],
        )
        if m.matches(node, keyword_form) or m.matches(node, positional_form):
            self.match_found = True


class PandasPlotSortColumnsRemoved(StatementLineCommentWriter):
    """In Spark 4.0, the sort_columns parameter from DataFrame.plot and Series.plot has been removed from pandas API on
    Spark.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-024",
            comment=f"As of PySpark {pyspark_version}, the sort_columns parameter of DataFrame.plot and Series.plot \
has been removed from pandas API on Spark.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if a plot call uses the removed sort_columns keyword argument"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.Name("plot")),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name("sort_columns")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class PandasToLatexColSpaceRemoved(StatementLineCommentWriter):
    """In Spark 4.0, the col_space parameter from DataFrame.to_latex and Series.to_latex has been removed from pandas
    API on Spark.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-025",
            comment=f"As of PySpark {pyspark_version}, the col_space parameter of to_latex has been removed from \
pandas API on Spark.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if to_latex is called with the removed col_space keyword argument"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.Name("to_latex")),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name("col_space")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class PandasToExcelParamsRemoved(StatementLineCommentWriter):
    """In Spark 4.0, the encoding and verbose parameters from DataFrame.to_excel and Series.to_excel have been removed
    from pandas API on Spark.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-026",
            comment=f"As of PySpark {pyspark_version}, the encoding and verbose parameters of to_excel have been \
removed from pandas API on Spark.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if to_excel is called with the removed encoding / verbose keyword arguments"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.Name("to_excel")),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.OneOf(m.Name("encoding"), m.Name("verbose"))),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class PandasReadCsvExcelParamsRemoved(StatementLineCommentWriter):
    """In Spark 4.0, the squeeze and mangle_dupe_cols parameters from ps.read_csv / ps.read_excel and the
    convert_float parameter from ps.read_excel have been removed from pandas API on Spark.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-027",
            comment=f"As of PySpark {pyspark_version}, the squeeze, mangle_dupe_cols and convert_float parameters of \
ps.read_csv / ps.read_excel have been removed from pandas API on Spark.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if read_csv / read_excel is called with a removed keyword argument"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Name("read_csv"),
                    m.Name("read_excel"),
                    m.Attribute(attr=m.Name("read_csv")),
                    m.Attribute(attr=m.Name("read_excel")),
                ),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(
                        keyword=m.OneOf(
                            m.Name("squeeze"),
                            m.Name("mangle_dupe_cols"),
                            m.Name("convert_float"),
                        )
                    ),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class PandasInfoNullCountsRemoved(StatementLineCommentWriter):
    """In Spark 4.0, the null_counts parameter from DataFrame.info has been removed from pandas API on Spark, use
    show_counts instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-028",
            comment=f"As of PySpark {pyspark_version}, the null_counts parameter of DataFrame.info has been removed \
from pandas API on Spark, use show_counts instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if info is called with the removed null_counts keyword argument"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.Name("info")),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name("null_counts")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class AssertPandasOnSparkEqualRemoved(StatementLineCommentWriter):
    """In Spark 4.0, pyspark.testing.assertPandasOnSparkEqual has been removed, use
    pyspark.pandas.testing.assert_frame_equal instead.
    """

    def __init__(
        self,
        pyspark_version: str = "4.0",
    ):
        super().__init__(
            transformer_id="PY35-40-029",
            comment=f"As of PySpark {pyspark_version}, pyspark.testing.assertPandasOnSparkEqual has been removed, \
use pyspark.pandas.testing.assert_frame_equal instead.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if the removed assertPandasOnSparkEqual function is being called"""
        if m.matches(
            node,
            m.Call(
                func=m.OneOf(
                    m.Name("assertPandasOnSparkEqual"),
                    m.Attribute(attr=m.Name("assertPandasOnSparkEqual")),
                )
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
        Python38SupportDropped(),
        PandasAppendRemoved(),
        PandasMadRemoved(),
        PandasToSparkIoRemoved(),
        PandasGetDtypeCountsRemoved(),
        PandasKoalasAccessorRemoved(),
        PandasIndexApisRemoved(),
        PandasIsMonotonicRemoved(),
        PandasWeekOfYearRemoved(),
        PandasGroupByBackfillPadRemoved(),
        PandasCategoricalInplaceRemoved(),
        PandasDateRangeClosedRemoved(),
        PandasBetweenTimeInclusiveRemoved(),
        PandasSeriesBetweenBooleanInclusiveRemoved(),
        PandasPlotSortColumnsRemoved(),
        PandasToLatexColSpaceRemoved(),
        PandasToExcelParamsRemoved(),
        PandasReadCsvExcelParamsRemoved(),
        PandasInfoNullCountsRemoved(),
        AssertPandasOnSparkEqualRemoved(),
    ]
