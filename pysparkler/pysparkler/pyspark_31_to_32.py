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
    StatementLineCommentWriter,
    is_dotted_name,
)


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


class FunctionsSnakeCaseRenamed(StatementLineCommentWriter):
    """In PySpark 3.2, several camelCase functions in pyspark.sql.functions were deprecated in favour of snake_case
    replacements and now emit a FutureWarning (sumDistinct, bitwiseNOT, shiftLeft, shiftRight, shiftRightUnsigned);
    approxCountDistinct, toDegrees and toRadians have had snake_case replacements since 2.1. The signatures are
    identical, so the references are renamed in place: attributes on a bound pyspark.sql.functions module, names in
    ``from pyspark.sql.functions import ...`` and every bare reference to such an (unaliased) imported name. Names
    imported with an explicit ``as`` alias keep their stable local name. Note that sum_distinct, bitwise_not,
    shiftleft, shiftright and shiftrightunsigned only exist in PySpark 3.2 and later.
    """

    renames = {
        "sumDistinct": "sum_distinct",
        "bitwiseNOT": "bitwise_not",
        "shiftLeft": "shiftleft",
        "shiftRight": "shiftright",
        "shiftRightUnsigned": "shiftrightunsigned",
        "approxCountDistinct": "approx_count_distinct",
        "toDegrees": "degrees",
        "toRadians": "radians",
    }

    def __init__(self, pyspark_version: str = "3.2"):
        super().__init__(
            transformer_id="PY31-32-002",
            comment=f"Renamed to the snake_case function added in PySpark {pyspark_version} (the camelCase alias \
emits FutureWarning; sum_distinct/bitwise_not/shiftleft/shiftright/shiftrightunsigned require PySpark >= \
{pyspark_version}).",
        )
        self._imports = ModuleImportCollector("pyspark", "sql", "functions")
        self._not_references: set[int] = set()

    def visit_Module(self, node: cst.Module) -> None:
        """Pre-scan the module for pyspark.sql.functions bindings and user-defined names"""
        self._imports = ModuleImportCollector("pyspark", "sql", "functions")
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

    def _renameable_import(self, name: str) -> bool:
        """A name is renamed (in the import and in every bare reference) only when it was imported from
        pyspark.sql.functions without an explicit alias and the module does not rebind it
        """
        return (
            name in self.renames
            and self._imports.is_imported_name(name, name)
            and name not in self._imports.explicit_aliases
        )

    def leave_ImportFrom(
        self,
        original_node: cst.ImportFrom,  # pylint: disable=unused-argument
        updated_node: cst.ImportFrom,
    ) -> cst.ImportFrom:
        """Rename ``from pyspark.sql.functions import shiftLeft`` (aliases with ``as`` are left untouched)"""
        if (
            updated_node.module is None
            or not is_dotted_name(updated_node.module, "pyspark", "sql", "functions")
            or isinstance(updated_node.names, cst.ImportStar)
        ):
            return updated_node
        names = []
        changed = False
        for alias in updated_node.names:
            if (
                isinstance(alias.name, cst.Name)
                and alias.asname is None
                and self._renameable_import(alias.name.value)
            ):
                alias = alias.with_changes(
                    name=alias.name.with_changes(value=self.renames[alias.name.value])
                )
                changed = True
            names.append(alias)
        if changed:
            self.match_found = True
            return updated_node.with_changes(names=names)
        return updated_node

    def leave_Attribute(
        self,
        original_node: cst.Attribute,  # pylint: disable=unused-argument
        updated_node: cst.Attribute,
    ) -> cst.Attribute:
        """Rename ``F.shiftLeft`` / ``pyspark.sql.functions.shiftLeft`` on a bound functions module"""
        if updated_node.attr.value in self.renames and self._imports.binds_module(
            updated_node.value
        ):
            self.match_found = True
            return updated_node.with_changes(
                attr=updated_node.attr.with_changes(
                    value=self.renames[updated_node.attr.value]
                )
            )
        return updated_node

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        """Rename every bare reference (calls, ``fn = shiftLeft``, ``partial(shiftLeft, ...)``, ...) to a name that
        was imported unaliased from pyspark.sql.functions, so the renamed import stays consistent
        """
        if id(original_node) in self._not_references or not self._renameable_import(
            updated_node.value
        ):
            return updated_node
        self.match_found = True
        return updated_node.with_changes(value=self.renames[updated_node.value])


def pyspark_31_to_32_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 3.1 to 3.2 migration guide"""
    return [SqlMlMethodsRaiseTypeErrorCommentWriter(), FunctionsSnakeCaseRenamed()]
