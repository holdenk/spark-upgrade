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
    ModuleImportCollector,
    RequiredDependencyVersionCommentWriter,
    StatementLineCommentWriter,
    escape_literal_for_comment,
    is_dotted_name,
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


class DateTimePatternLettersChanged(StatementLineCommentWriter):
    """In Spark 3.0, datetime parsing/formatting switched from SimpleDateFormat to java.time.DateTimeFormatter.
    The week-based pattern letters Y, W, w, u, e and c are rejected (SparkUpgradeException) and F changed its meaning
    from week-of-month to aligned day-of-week-in-month. This rule flags literal patterns passed to the
    pyspark.sql.functions date_format, to_date, to_timestamp, unix_timestamp and from_unixtime functions (the module
    must be imported, so pandas/arrow methods with the same names are not flagged) and to the dateFormat /
    timestampFormat reader and writer options.
    """

    datetime_functions = {
        "date_format",
        "to_date",
        "to_timestamp",
        "unix_timestamp",
        "from_unixtime",
    }
    pattern_options = {"dateFormat", "timestampFormat"}
    problem_letters = "YWwuecF"

    def __init__(self, pyspark_version: str = "3.0"):
        super().__init__(transformer_id="PY24-30-009", comment="")
        self.pyspark_version = pyspark_version
        self._imports = ModuleImportCollector("pyspark", "sql", "functions")
        self._pending: list[tuple[str, list[str]]] = []

    def visit_Module(self, node: cst.Module) -> None:
        """Pre-scan the module for pyspark.sql.functions bindings"""
        self._imports = ModuleImportCollector("pyspark", "sql", "functions")
        node.visit(self._imports)
        self._pending = []

    @classmethod
    def week_based_letters(cls, pattern: str) -> list[str]:
        """Return the problematic pattern letters that appear outside single-quoted text (``''`` is an escaped
        quote). Patterns containing ``%`` directives (strftime style) are not Spark patterns and are skipped.
        """
        if "%" in pattern:
            return []
        letters: list[str] = []
        in_quotes = False
        i = 0
        while i < len(pattern):
            ch = pattern[i]
            if ch == "'":
                if pattern.startswith("'", i + 1):
                    i += 2  # escaped single quote
                    continue
                in_quotes = not in_quotes
            elif not in_quotes and ch in cls.problem_letters and ch not in letters:
                letters.append(ch)
            i += 1
        return letters

    @staticmethod
    def _literal(node: cst.BaseExpression) -> str | None:
        if isinstance(node, cst.SimpleString):
            value = node.evaluated_value
            if isinstance(value, str):
                return value
        return None

    def _is_sql_datetime_function(self, func: cst.BaseExpression) -> bool:
        """Return True if the called function is one of the pyspark.sql.functions datetime functions"""
        if isinstance(func, cst.Name):
            if func.value in self._imports.redefined_names:
                return False
            if self._imports.star_import and func.value in self.datetime_functions:
                return True
            return self._imports.is_imported_name(func.value, *self.datetime_functions)
        if isinstance(func, cst.Attribute):
            return (
                func.attr.value in self.datetime_functions
                and self._imports.binds_module(func.value)
            )
        return False

    def _pattern_from_function_call(self, node: cst.Call) -> str | None:
        """Return the literal pattern of a datetime function call, if any"""
        if not self._is_sql_datetime_function(node.func):
            return None
        for arg in node.args:
            if m.matches(arg, m.Arg(keyword=m.Name("format"))):
                return self._literal(arg.value)
        positional = [arg for arg in node.args if arg.keyword is None and not arg.star]
        if len(positional) > 1 and len(positional) == len(node.args):
            return self._literal(positional[1].value)
        return None

    def _patterns_from_options(self, node: cst.Call) -> list[str]:
        """Return literal dateFormat / timestampFormat patterns given through .option(...) or keyword options"""
        patterns: list[str] = []
        if (
            m.matches(
                node,
                m.Call(
                    func=m.Attribute(attr=m.Name("option")),
                    args=[
                        m.Arg(value=m.SimpleString()),
                        m.Arg(value=m.SimpleString()),
                        m.ZeroOrMore(),
                    ],
                ),
            )
            and self._literal(node.args[0].value) in self.pattern_options
        ):
            pattern = self._literal(node.args[1].value)
            if pattern is not None:
                patterns.append(pattern)
        for arg in node.args:
            if arg.keyword is not None and arg.keyword.value in self.pattern_options:
                pattern = self._literal(arg.value)
                if pattern is not None:
                    patterns.append(pattern)
        return patterns

    def visit_Call(self, node: cst.Call) -> None:
        """Check datetime function calls and reader/writer options for literal week-based patterns"""
        patterns = self._patterns_from_options(node)
        function_pattern = self._pattern_from_function_call(node)
        if function_pattern is not None:
            patterns.append(function_pattern)
        for pattern in patterns:
            letters = self.week_based_letters(pattern)
            if letters:
                self._pending.append((pattern, letters))
                self.match_found = True

    def prepare_comment(self) -> None:
        """Build one message naming every flagged pattern on the line followed by the shared explanation"""
        if not self._pending:
            return
        found = ", ".join(
            f"'{escape_literal_for_comment(pattern)}' (letter(s) {', '.join(letters)})"
            for pattern, letters in self._pending
        )
        several = len(self._pending) > 1
        plural, verb = ("s", "use") if several else ("", "uses")
        self.comment = f"Datetime pattern{plural} {found} {verb} letters whose meaning changed in Spark \
{self.pyspark_version} (DateTimeFormatter replaced SimpleDateFormat): Y/W/w/u/e/c (week-based year, week-of-month, \
week-of-year, day-number-of-week, localized day-of-week) are rejected as week-based patterns and F now means \
aligned day-of-week-in-month (was week-of-month). Use y for the year, E for the day name or \
weekofyear()/dayofweek() for week computations, see \
https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html. To keep the 2.4 behaviour set \
spark.sql.legacy.timeParserPolicy=LEGACY."
        self._pending = []


class OneHotEncoderEstimatorRenamed(StatementLineCommentWriter):
    """In Spark 3.0, OneHotEncoderEstimator is renamed to OneHotEncoder and the old Transformer-style OneHotEncoder
    (deprecated in 2.3) is removed. The name is renamed in ``from pyspark.ml.feature import ...`` statements, in
    bare references to the name imported that way and in attribute references on a bound pyspark.ml.feature module;
    duplicate ``OneHotEncoder`` import aliases are dropped. Names that merely look alike (a user class, keyword
    arguments, attributes of other objects) are left alone.
    """

    old_name = "OneHotEncoderEstimator"
    new_name = "OneHotEncoder"

    def __init__(self, pyspark_version: str = "3.0"):
        super().__init__(
            transformer_id="PY24-30-010",
            comment=f"As of Spark {pyspark_version}, OneHotEncoderEstimator is renamed to OneHotEncoder; the old \
OneHotEncoder Transformer was removed.",
        )
        self._imports = ModuleImportCollector("pyspark", "ml", "feature")
        self._not_references: set[int] = set()

    def visit_Module(self, node: cst.Module) -> None:
        """Pre-scan the module for pyspark.ml.feature bindings"""
        self._imports = ModuleImportCollector("pyspark", "ml", "feature")
        node.visit(self._imports)
        self._not_references = set()

    # Name nodes that are not references to a module-level binding: attribute names, keyword argument names,
    # import aliases and parameter names. They are remembered by identity and skipped in leave_Name.
    def visit_Attribute(self, node: cst.Attribute) -> None:
        self._not_references.add(id(node.attr))

    def visit_Arg(self, node: cst.Arg) -> None:
        if node.keyword is not None:
            self._not_references.add(id(node.keyword))

    def visit_Param(self, node: cst.Param) -> None:
        self._not_references.add(id(node.name))

    def visit_ImportAlias(self, node: cst.ImportAlias) -> None:
        name: cst.BaseExpression = node.name
        while isinstance(name, cst.Attribute):
            self._not_references.add(id(name.attr))
            name = name.value
        self._not_references.add(id(name))
        if node.asname is not None:
            self._not_references.add(id(node.asname.name))

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        """Rename bare references to OneHotEncoderEstimator imported from pyspark.ml.feature"""
        if (
            updated_node.value == self.old_name
            and id(original_node) not in self._not_references
            and self._imports.is_imported_name(self.old_name, self.old_name)
        ):
            self.match_found = True
            return updated_node.with_changes(value=self.new_name)
        return updated_node

    def leave_Attribute(
        self,
        original_node: cst.Attribute,  # pylint: disable=unused-argument
        updated_node: cst.Attribute,
    ) -> cst.Attribute:
        """Rename ``feature.OneHotEncoderEstimator`` / ``pyspark.ml.feature.OneHotEncoderEstimator``"""
        if updated_node.attr.value == self.old_name and self._imports.binds_module(
            updated_node.value
        ):
            self.match_found = True
            return updated_node.with_changes(
                attr=updated_node.attr.with_changes(value=self.new_name)
            )
        return updated_node

    @staticmethod
    def _comma_comment(alias: cst.ImportAlias) -> str | None:
        """Return the inline comment carried by the comma of an alias in a parenthesised import, if any"""
        comma = alias.comma
        if isinstance(comma, cst.Comma) and isinstance(
            comma.whitespace_after, cst.ParenthesizedWhitespace
        ):
            comment = comma.whitespace_after.first_line.comment
            return comment.value if comment is not None else None
        return None

    @staticmethod
    def _with_comma_comment(comma: cst.Comma, comment: str | None) -> cst.Comma:
        whitespace_after = comma.whitespace_after
        if not isinstance(whitespace_after, cst.ParenthesizedWhitespace):
            return comma
        first_line = whitespace_after.first_line.with_changes(
            whitespace=cst.SimpleWhitespace("  ")
            if comment
            else first_line_whitespace(whitespace_after),
            comment=cst.Comment(comment) if comment else None,
        )
        return comma.with_changes(
            whitespace_after=whitespace_after.with_changes(first_line=first_line)
        )

    def _merge_dropped_alias(
        self, survivor: cst.ImportAlias, dropped: cst.ImportAlias
    ) -> cst.ImportAlias:
        """Keep the inline comment of a dropped duplicate alias by moving it onto the preceding surviving alias"""
        dropped_comment = self._comma_comment(dropped)
        if dropped_comment is None:
            return survivor
        survivor_comment = self._comma_comment(survivor)
        if survivor_comment is None and not (
            isinstance(survivor.comma, cst.Comma)
            and isinstance(survivor.comma.whitespace_after, cst.ParenthesizedWhitespace)
        ):
            # The survivor sits on the same line as the dropped alias: take over the dropped comma and its comment
            return survivor.with_changes(comma=dropped.comma)
        merged = (
            f"{survivor_comment}  {dropped_comment}"
            if survivor_comment
            else dropped_comment
        )
        assert isinstance(survivor.comma, cst.Comma)
        return survivor.with_changes(
            comma=self._with_comma_comment(survivor.comma, merged)
        )

    def _dedupe(
        self, node: cst.ImportFrom, names: list[cst.ImportAlias]
    ) -> cst.ImportFrom:
        """Drop unaliased ``OneHotEncoder`` aliases beyond the first one, keeping comments and the closing layout"""
        kept: list[cst.ImportAlias] = []
        dropped_last = False
        seen = False
        for index, alias in enumerate(names):
            if m.matches(alias, m.ImportAlias(name=m.Name(self.new_name), asname=None)):
                if seen:
                    if kept:
                        kept[-1] = self._merge_dropped_alias(kept[-1], alias)
                    dropped_last = index == len(names) - 1
                    continue
                seen = True
            kept.append(alias)
        if len(kept) == len(names):
            return node.with_changes(names=names)
        if dropped_last:
            last = names[-1]
            survivor = kept[-1]
            comment = self._comma_comment(survivor)
            if isinstance(last.comma, cst.Comma):
                # Trailing comma style: reuse the closing comma of the dropped alias, carrying the merged comment
                kept[-1] = survivor.with_changes(
                    comma=self._with_comma_comment(last.comma, comment)
                )
            elif comment is None:
                kept[-1] = survivor.with_changes(comma=cst.MaybeSentinel.DEFAULT)
            else:
                # Keep the comma (and its comment) and let the closing parenthesis follow on the next line
                assert isinstance(survivor.comma, cst.Comma)
                assert isinstance(
                    survivor.comma.whitespace_after, cst.ParenthesizedWhitespace
                )
                kept[-1] = survivor.with_changes(
                    comma=survivor.comma.with_changes(
                        whitespace_after=survivor.comma.whitespace_after.with_changes(
                            last_line=cst.SimpleWhitespace("")
                        )
                    )
                )
                if node.rpar is not None and isinstance(
                    node.rpar.whitespace_before, cst.ParenthesizedWhitespace
                ):
                    node = node.with_changes(
                        rpar=node.rpar.with_changes(
                            whitespace_before=cst.SimpleWhitespace("")
                        )
                    )
        return node.with_changes(names=kept)

    def leave_ImportFrom(
        self,
        original_node: cst.ImportFrom,  # pylint: disable=unused-argument
        updated_node: cst.ImportFrom,
    ) -> cst.ImportFrom:
        """Rename the alias in ``from pyspark.ml.feature import OneHotEncoderEstimator [as X]`` and deduplicate"""
        if (
            updated_node.module is None
            or not is_dotted_name(updated_node.module, "pyspark", "ml", "feature")
            or isinstance(updated_node.names, cst.ImportStar)
        ):
            return updated_node
        names: list[cst.ImportAlias] = []
        changed = False
        for alias in updated_node.names:
            if m.matches(alias.name, m.Name(self.old_name)):
                assert isinstance(alias.name, cst.Name)
                alias = alias.with_changes(
                    name=alias.name.with_changes(value=self.new_name)
                )
                changed = True
            names.append(alias)
        if not changed:
            return updated_node
        self.match_found = True
        return self._dedupe(updated_node, names)


def first_line_whitespace(
    whitespace: cst.ParenthesizedWhitespace,
) -> cst.SimpleWhitespace:
    """The whitespace preceding the (possibly absent) comment on the first line of a parenthesized whitespace"""
    return whitespace.first_line.whitespace


class OneHotEncoderTransformWithoutFit(StatementLineCommentWriter):
    """In Spark 3.0, OneHotEncoder is an Estimator (the Transformer-style OneHotEncoder was removed), so
    ``OneHotEncoder(...).transform(df)`` fails: the encoder must be fit first. Only fires when the module imports
    OneHotEncoder from pyspark.ml.feature and not from another library such as scikit-learn. Besides the inline
    form, the two-step idiom ``enc = OneHotEncoder(...)`` followed by ``enc.transform(df)`` is flagged when ``enc``
    is bound exactly once in the module.
    """

    def __init__(self, pyspark_version: str = "3.0"):
        super().__init__(
            transformer_id="PY24-30-011",
            comment=f"As of Spark {pyspark_version}, OneHotEncoder is an Estimator (the Transformer-style \
OneHotEncoder was removed); call OneHotEncoder(...).fit(df).transform(df) or use it inside a Pipeline.",
        )
        self._imports = ModuleImportCollector("pyspark", "ml", "feature")
        self._encoder_variables: set[str] = set()

    def visit_Module(self, node: cst.Module) -> None:
        """Pre-scan the module for pyspark.ml.feature bindings and for variables holding an unfitted encoder"""
        self._imports = ModuleImportCollector("pyspark", "ml", "feature")
        node.visit(self._imports)
        self._encoder_variables = set()
        for assign in m.findall(
            node, m.Assign(targets=[m.AssignTarget(target=m.Name())], value=m.Call())
        ):
            assert isinstance(assign, cst.Assign) and isinstance(assign.value, cst.Call)
            target = assign.targets[0].target
            assert isinstance(target, cst.Name)
            if self._imports.binding_counts.get(
                target.value
            ) == 1 and self._is_pyspark_one_hot_encoder(assign.value.func):
                self._encoder_variables.add(target.value)

    def _is_pyspark_one_hot_encoder(self, func: cst.BaseExpression) -> bool:
        if isinstance(func, cst.Name):
            if func.value in self._imports.redefined_names:
                return False
            if self._imports.star_import and func.value == "OneHotEncoder":
                return True
            return self._imports.is_imported_name(
                func.value, "OneHotEncoder", "OneHotEncoderEstimator"
            )
        if isinstance(func, cst.Attribute):
            return func.attr.value in (
                "OneHotEncoder",
                "OneHotEncoderEstimator",
            ) and self._imports.binds_module(func.value)
        return False

    def visit_Call(self, node: cst.Call) -> None:
        """Check for OneHotEncoder(...).transform(...) (or enc = OneHotEncoder(...); enc.transform(...)) without fit"""
        if not m.matches(node, m.Call(func=m.Attribute(attr=m.Name("transform")))):
            return
        receiver = node.func.value  # type: ignore[attr-defined]
        if isinstance(receiver, cst.Call) and self._is_pyspark_one_hot_encoder(
            receiver.func
        ):
            self.match_found = True
        elif (
            isinstance(receiver, cst.Name) and receiver.value in self._encoder_variables
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
        DateTimePatternLettersChanged(),
        OneHotEncoderEstimatorRenamed(),
        OneHotEncoderTransformWithoutFit(),
    ]
