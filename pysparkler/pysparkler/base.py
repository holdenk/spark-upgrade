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
from typing import Any

import libcst as cst
import libcst.matchers as m


class BaseTransformer(m.MatcherDecoratableTransformer):
    """Base class for all transformers.

    Properties:
        transformer_id: A unique identifier for the transformer rule. Follows the format
            PY<From-Major-Version>-<To-Major-Version>-<Rule-Number>
            Important for idempotency checks and debugging.
        enabled: A boolean to enable or disable the transformer rule
    """

    def __init__(self, transformer_id: str, enabled: bool = True):
        super().__init__()
        self._transformer_id = transformer_id
        self._enabled = enabled

    @property
    def transformer_id(self) -> str:
        """A unique read-only identifier for the transformer rule"""
        return self._transformer_id

    @transformer_id.setter
    def transformer_id(self, value):
        raise AttributeError("Cannot set the read-only transformer_id attribute")

    @property
    def enabled(self) -> bool:
        """A boolean to enable or disable the transformer rule"""
        return self._enabled

    @enabled.setter
    def enabled(self, value):
        self._enabled = value

    def override(self, **overrides: dict[str, Any]) -> "BaseTransformer":
        """Override the transformer attributes with kwargs passed in"""
        for key, value in overrides.items():
            # Iterate over the kwargs and override the existing attributes
            setattr(self, key, value)
        return self


class StatementLineCommentWriter(BaseTransformer):
    """Base class for adding comments to the end of the statement line when a matching condition is found.

    A match found while visiting the header of a compound statement (``if``/``for``/``while``/``with``/``def``/
    ``class`` and their decorators) is attached to that header line (after the colon) rather than to the first
    statement of the body.
    """

    def __init__(
        self,
        transformer_id: str,
        comment: str,
    ):
        super().__init__(transformer_id)
        self._comment = comment
        self.match_found = False
        self._header_comments: list[str | None] = []

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    def prepare_comment(self) -> None:
        """Hook called right before a comment is emitted. Rules that compose their message from the matches
        collected on the current line override this to set ``self.comment``."""

    def _formatted_comment(self) -> str:
        self.prepare_comment()
        return f"# {self.transformer_id}: {self.comment}"

    def leave_SimpleStatementLine(
        self,
        original_node: cst.SimpleStatementLine,  # pylint: disable=unused-argument
        updated_node: cst.SimpleStatementLine,
    ) -> cst.SimpleStatementLine:
        """Add a comment to the end of the statement line if a matching condition is found"""
        if self.match_found:
            self.match_found = False
            return add_comment_to_end_of_a_simple_statement_line(
                updated_node,
                self.transformer_id,
                self._formatted_comment(),
            )
        else:
            return updated_node

    def visit_IndentedBlock(
        self,
        node: cst.IndentedBlock,  # pylint: disable=unused-argument
    ) -> None:
        """A match already pending when a body starts came from the enclosing compound statement header: remember
        its comment for the header line and do not let it leak onto the first body statement
        """
        if self.match_found:
            self.match_found = False
            self._header_comments.append(self._formatted_comment())
        else:
            self._header_comments.append(None)

    def leave_IndentedBlock(
        self,
        original_node: cst.IndentedBlock,  # pylint: disable=unused-argument
        updated_node: cst.IndentedBlock,
    ) -> cst.IndentedBlock:
        """Attach a header match comment to the text following the colon of the compound statement"""
        comment = self._header_comments.pop() if self._header_comments else None
        if comment is None:
            return updated_node
        return updated_node.with_changes(
            header=add_comment_to_trailing_whitespace(
                updated_node.header, self.transformer_id, comment
            )
        )

    def visit_SimpleStatementSuite(self, node: cst.SimpleStatementSuite) -> None:
        """Same as visit_IndentedBlock for one-line compound statements such as ``if x: y = 1``"""
        self.visit_IndentedBlock(node)  # type: ignore[arg-type]

    def leave_SimpleStatementSuite(
        self,
        original_node: cst.SimpleStatementSuite,  # pylint: disable=unused-argument
        updated_node: cst.SimpleStatementSuite,
    ) -> cst.SimpleStatementSuite:
        """Attach a header match, or a match inside the one-line body, to the end of that line"""
        comment = self._header_comments.pop() if self._header_comments else None
        if comment is None and self.match_found:
            self.match_found = False
            comment = self._formatted_comment()
        if comment is None:
            return updated_node
        return updated_node.with_changes(
            trailing_whitespace=add_comment_to_trailing_whitespace(
                updated_node.trailing_whitespace, self.transformer_id, comment
            )
        )


class PySparkImportCommentWriter(StatementLineCommentWriter):
    """Base class for adding a single comment to the first PySpark import in a module.

    Matches any ``import pyspark...`` statement (including dotted submodules and
    multi-name imports such as ``import os, pyspark``) and any
    ``from pyspark... import ...`` statement. It fires only once per module so that a
    file with several PySpark imports receives the hint a single time.
    """

    def __init__(self, transformer_id: str, comment: str):
        super().__init__(transformer_id=transformer_id, comment=comment)
        self._already_flagged = False

    @staticmethod
    def _root_is_pyspark(node: cst.BaseExpression | None) -> bool:
        """Return True if a dotted module path (Name/Attribute) is rooted at ``pyspark``."""
        while isinstance(node, cst.Attribute):
            node = node.value
        return isinstance(node, cst.Name) and node.value == "pyspark"

    def visit_Import(self, node: cst.Import) -> None:
        """Check for ``import pyspark...`` (possibly alongside other modules)"""
        if not self._already_flagged and any(
            self._root_is_pyspark(alias.name) for alias in node.names
        ):
            self.match_found = True

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        """Check for ``from pyspark... import ...``"""
        if not self._already_flagged and self._root_is_pyspark(node.module):
            self.match_found = True

    def leave_SimpleStatementLine(
        self,
        original_node: cst.SimpleStatementLine,
        updated_node: cst.SimpleStatementLine,
    ) -> cst.SimpleStatementLine:
        """Emit the comment once, then suppress further matches in this module"""
        if self.match_found:
            self._already_flagged = True
        return super().leave_SimpleStatementLine(original_node, updated_node)


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
            comment=f"PySpark {pyspark_version} requires {required_dependency_name} version \
{required_dependency_version} or higher",
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

    @comment.setter
    def comment(self, value):
        self._comment = value

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


def add_comment_to_trailing_whitespace(
    trailing_whitespace: cst.TrailingWhitespace, transformer_id: str, comment: str
) -> cst.TrailingWhitespace:
    """Appends a comment to a trailing whitespace node (the end of a simple statement line or the text after the
    colon of a compound statement header). An existing comment is kept and the new one appended after it, unless
    the transformer already left its comment there. Newlines are never allowed inside a comment, so the text is
    sanitised to keep the resulting module valid."""

    ignore_line_too_long = "# noqa: E501"
    comment = sanitize_comment(comment)
    if trailing_whitespace.comment:
        # If there is already a comment
        original_comment = trailing_whitespace.comment.value
        if transformer_id in original_comment:
            # If the comment is already added by this transformer, do nothing
            return trailing_whitespace
        else:
            # Add the comment to the end of the comments
            new_cm = f"{original_comment.replace(ignore_line_too_long, '').rstrip()}  {comment}  {ignore_line_too_long}"
            return trailing_whitespace.with_changes(
                comment=trailing_whitespace.comment.with_changes(value=new_cm),
            )
    else:
        # If there is no comment, add a comment to the trailing whitespace
        return trailing_whitespace.with_changes(
            whitespace=cst.SimpleWhitespace(value="  "),
            comment=cst.Comment(value=f"{comment}  {ignore_line_too_long}"),
        )


def sanitize_comment(comment: str) -> str:
    """Replace characters that cannot appear inside a ``#`` comment (line breaks) with a space"""
    return " ".join(comment.replace("\r\n", "\n").replace("\r", "\n").split("\n"))


def escape_literal_for_comment(value: str) -> str:
    """Return a user string literal in a form that is safe to interpolate into a hint comment: control characters
    such as newlines and tabs are shown escaped (``\\n``) instead of being emitted"""
    return (
        value.replace("\\", "\\\\")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
    )


def add_comment_to_end_of_a_simple_statement_line(
    node: cst.SimpleStatementLine, transformer_id: str, comment: str
) -> cst.SimpleStatementLine:
    """Adds a comment to the end of a statement line"""
    return node.with_changes(
        trailing_whitespace=add_comment_to_trailing_whitespace(
            node.trailing_whitespace, transformer_id, comment
        )
    )


def one_of_matching_strings(*strings: str) -> m.OneOf[m.SimpleString]:
    """Returns a one of matcher that matches a string regardless of the quotes used"""
    return m.OneOf(
        *[
            m.SimpleString(value=quoted_str)
            for string in strings
            for quoted_str in (f'"{string}"', f"'{string}'")
        ]
    )


def is_dotted_name(node: cst.BaseExpression | None, *parts: str) -> bool:
    """Return True if a Name/Attribute chain spells exactly the dotted module path ``parts``
    (e.g. ``is_dotted_name(node, "pyspark", "sql", "functions")``)"""
    if node is None:
        return False
    names: list[str] = []
    while isinstance(node, cst.Attribute):
        names.append(node.attr.value)
        node = node.value
    if not isinstance(node, cst.Name):
        return False
    names.append(node.value)
    return tuple(reversed(names)) == parts


class ModuleImportCollector(cst.CSTVisitor):
    """Collects how a given dotted module (e.g. ``pyspark.sql.functions``) is bound inside a module.

    After visiting a ``cst.Module`` the following attributes are populated:

    - ``module_aliases``: local names bound to the module itself, e.g. ``F`` for
      ``import pyspark.sql.functions as F`` or ``from pyspark.sql import functions as F``, and ``functions``
      for ``from pyspark.sql import functions``. A bare ``import pyspark.sql.functions`` binds no alias; use
      :func:`is_dotted_name` on the attribute chain instead.
    - ``imported_names``: ``{local_name: original_name}`` for every ``from <module> import name [as local]``.
    - ``explicit_aliases``: the local names among ``imported_names`` that were bound with an explicit ``as``
      (``from <module> import name as name`` included), so the local name is stable and must not be renamed.
    - ``star_import``: True when ``from <module> import *`` is present.
    - ``binding_counts``: how many times the module itself binds each name through ``def``, ``class``, function
      parameters, assignment / augmented assignment / annotated assignment / walrus targets, ``for`` and
      comprehension targets, ``with ... as`` targets and ``import ... as`` aliases.
    - ``redefined_names``: the names in ``binding_counts``; references to them must not be treated as references to
      the imported module members.

    The collector is scope-insensitive on purpose: a name rebound anywhere in the module is considered redefined.
    """

    def __init__(self, *module_parts: str):
        super().__init__()
        self._module_parts = module_parts
        self.module_aliases: set[str] = set()
        self.imported_names: dict[str, str] = {}
        self.explicit_aliases: set[str] = set()
        self.star_import = False
        self.binding_counts: dict[str, int] = {}

    @property
    def redefined_names(self) -> set[str]:
        """Names the module binds itself (outside the tracked imports)"""
        return set(self.binding_counts)

    def binds_module(self, node: cst.BaseExpression) -> bool:
        """Return True if the expression refers to the module, either through a bound alias (that the module does
        not rebind to something else) or its full dotted path"""
        if isinstance(node, cst.Name):
            return (
                node.value in self.module_aliases
                and node.value not in self.binding_counts
            )
        return is_dotted_name(node, *self._module_parts)

    def is_imported_name(self, local_name: str, *original_names: str) -> bool:
        """Return True if ``local_name`` was imported from the module (``from <module> import x [as local]``) as one
        of ``original_names`` (or any name when none is given) and is not rebound by the module itself
        """
        original = self.imported_names.get(local_name)
        if original is None or local_name in self.binding_counts:
            return False
        return not original_names or original in original_names

    def _bind(
        self, target: cst.BaseExpression | cst.BaseAssignTargetExpression
    ) -> None:
        """Record a binding of every Name inside an assignment-like target"""
        if isinstance(target, cst.Name):
            self.binding_counts[target.value] = (
                self.binding_counts.get(target.value, 0) + 1
            )
        elif isinstance(target, (cst.Tuple, cst.List)):
            for element in target.elements:
                self._bind(element.value)
        elif isinstance(target, cst.StarredElement):
            self._bind(target.value)

    def visit_Import(self, node: cst.Import) -> None:
        for alias in node.names:
            if alias.asname is None:
                continue
            if is_dotted_name(alias.name, *self._module_parts):
                if isinstance(alias.asname.name, cst.Name):
                    self.module_aliases.add(alias.asname.name.value)
            elif isinstance(alias.asname.name, cst.Name):
                self._bind(alias.asname.name)

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        if node.relative or node.module is None:
            if not node.relative or isinstance(node.names, cst.ImportStar):
                return
            for alias in node.names:
                local = alias.asname.name if alias.asname else alias.name
                if isinstance(local, cst.Name):
                    self._bind(local)
            return
        parent, last = self._module_parts[:-1], self._module_parts[-1]
        if parent and is_dotted_name(node.module, *parent):
            # ``from pyspark.sql import functions [as F]``
            if isinstance(node.names, cst.ImportStar):
                return
            for alias in node.names:
                local = alias.asname.name if alias.asname else alias.name
                if not isinstance(local, cst.Name):
                    continue
                if m.matches(alias.name, m.Name(last)):
                    self.module_aliases.add(local.value)
                else:
                    self._bind(local)
        elif is_dotted_name(node.module, *self._module_parts):
            # ``from pyspark.sql.functions import name [as local]`` / ``import *``
            if isinstance(node.names, cst.ImportStar):
                self.star_import = True
                return
            for alias in node.names:
                if isinstance(alias.name, cst.Name):
                    local = alias.asname.name if alias.asname else alias.name
                    if isinstance(local, cst.Name):
                        self.imported_names[local.value] = alias.name.value
                        if alias.asname is not None:
                            self.explicit_aliases.add(local.value)
        else:
            # Names imported from any other module are bindings of the module itself
            if isinstance(node.names, cst.ImportStar):
                return
            for alias in node.names:
                local = alias.asname.name if alias.asname else alias.name
                if isinstance(local, cst.Name):
                    self._bind(local)

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        self._bind(node.name)

    def visit_ClassDef(self, node: cst.ClassDef) -> None:
        self._bind(node.name)

    def visit_Param(self, node: cst.Param) -> None:
        self._bind(node.name)

    def visit_AssignTarget(self, node: cst.AssignTarget) -> None:
        self._bind(node.target)

    def visit_AnnAssign(self, node: cst.AnnAssign) -> None:
        self._bind(node.target)

    def visit_AugAssign(self, node: cst.AugAssign) -> None:
        self._bind(node.target)

    def visit_NamedExpr(self, node: cst.NamedExpr) -> None:
        self._bind(node.target)

    def visit_For(self, node: cst.For) -> None:
        self._bind(node.target)

    def visit_CompFor(self, node: cst.CompFor) -> None:
        self._bind(node.target)

    def visit_WithItem(self, node: cst.WithItem) -> None:
        if node.asname is not None:
            self._bind(node.asname.name)
