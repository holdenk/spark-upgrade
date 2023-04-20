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

    @comment.setter
    def comment(self, value):
        self._comment = value

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
                f"# {self.transformer_id}: {self.comment}",
            )
        else:
            return updated_node


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


def add_comment_to_end_of_a_simple_statement_line(
    node: cst.SimpleStatementLine, transformer_id: str, comment: str
) -> cst.SimpleStatementLine:
    """Adds a comment to the end of a statement line"""

    ignore_line_too_long = "# noqa: E501"
    if node.trailing_whitespace.comment:
        # If there is already a comment
        original_comment = node.trailing_whitespace.comment.value
        if transformer_id in original_comment:
            # If the comment is already added by this transformer, do nothing
            return node
        else:
            # Add the comment to the end of the comments
            new_cm = f"{original_comment.replace(ignore_line_too_long, '').rstrip()}  {comment}  {ignore_line_too_long}"
            return node.with_changes(
                trailing_whitespace=cst.TrailingWhitespace(
                    whitespace=node.trailing_whitespace.whitespace,
                    comment=node.trailing_whitespace.comment.with_changes(value=new_cm),
                    newline=node.trailing_whitespace.newline,
                )
            )
    else:
        # If there is no comment, add a comment to the trailing whitespace
        return node.with_changes(
            trailing_whitespace=cst.TrailingWhitespace(
                whitespace=cst.SimpleWhitespace(
                    value="  ",
                ),
                comment=cst.Comment(value=f"{comment}  {ignore_line_too_long}"),
                newline=cst.Newline(
                    value=None,
                ),
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
