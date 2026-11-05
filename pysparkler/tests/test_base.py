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

from pysparkler.base import (
    ModuleImportCollector,
    StatementLineCommentWriter,
    add_comment_to_trailing_whitespace,
    escape_literal_for_comment,
    sanitize_comment,
)
from tests.conftest import rewrite


def test_writes_comment_with_noqa_when_match_is_found():
    given_code = """
import pyspark
"""
    comment_writer = StatementLineCommentWriter(transformer_id="foo", comment="bar")
    comment_writer.match_found = True

    modified_code = rewrite(given_code, comment_writer)
    expected_code = """
import pyspark  # foo: bar  # noqa: E501
"""
    assert modified_code == expected_code


def test_overrides_comment_from_kwargs():
    given_code = """
import pyspark
"""
    overrides = {"comment": "baz"}
    comment_writer = StatementLineCommentWriter(transformer_id="foo", comment="bar")
    comment_writer.match_found = True
    comment_writer.override(**overrides)

    modified_code = rewrite(given_code, comment_writer)
    expected_code = """
import pyspark  # foo: baz  # noqa: E501
"""
    assert modified_code == expected_code


def test_overrides_with_unknown_attributes_are_silently_ignored():
    given_code = """
import pyspark
"""
    overrides = {"unknown": "attribute"}
    comment_writer = StatementLineCommentWriter(transformer_id="foo", comment="bar")
    comment_writer.match_found = True
    comment_writer.override(**overrides)

    modified_code = rewrite(given_code, comment_writer)
    expected_code = """
import pyspark  # foo: bar  # noqa: E501
"""
    assert modified_code == expected_code


class _FlagCalls(StatementLineCommentWriter):
    """Test transformer flagging every call to ``flag(...)``"""

    def __init__(self):
        super().__init__(transformer_id="TST-001", comment="flagged")

    def visit_Call(self, node: cst.Call) -> None:
        if cst.matchers.matches(
            node, cst.matchers.Call(func=cst.matchers.Name("flag"))
        ):
            self.match_found = True


def test_comment_text_with_line_breaks_is_sanitized_and_module_stays_valid():
    trailing = add_comment_to_trailing_whitespace(
        cst.TrailingWhitespace(), "TST", "# TST: first line\nsecond\r\nthird\rfourth"
    )
    assert trailing.comment is not None
    assert (
        trailing.comment.value == "# TST: first line second third fourth  # noqa: E501"
    )
    assert sanitize_comment("a\nb") == "a b"
    assert escape_literal_for_comment("YYYY\nMM\t\\x") == "YYYY\\nMM\\t\\\\x"


def test_comment_is_appended_to_an_existing_comment_once():
    trailing = cst.TrailingWhitespace(
        whitespace=cst.SimpleWhitespace("  "), comment=cst.Comment("# original")
    )
    once = add_comment_to_trailing_whitespace(trailing, "TST", "# TST: hint")
    twice = add_comment_to_trailing_whitespace(once, "TST", "# TST: hint")
    assert once.comment is not None
    assert once.comment.value == "# original  # TST: hint  # noqa: E501"
    assert twice == once


def test_header_match_comment_is_attached_to_the_compound_statement_header():
    given_code = """
if flag(1):
    x = 1
for row in flag(2):
    '''docstring-like body'''
@decorator(flag(3))
def g(c=flag(4)):
    pass
class K(Base, metaclass=flag(5)):
    y = 2
while flag(6): z = 1
if other: w = flag(7)
try:
    pass
except flag(8):
    pass
last = 1
"""
    modified_code = rewrite(given_code, _FlagCalls())
    expected_code = """
if flag(1):  # TST-001: flagged  # noqa: E501
    x = 1
for row in flag(2):  # TST-001: flagged  # noqa: E501
    '''docstring-like body'''
@decorator(flag(3))
def g(c=flag(4)):  # TST-001: flagged  # noqa: E501
    pass
class K(Base, metaclass=flag(5)):  # TST-001: flagged  # noqa: E501
    y = 2
while flag(6): z = 1  # TST-001: flagged  # noqa: E501
if other: w = flag(7)  # TST-001: flagged  # noqa: E501
try:
    pass
except flag(8):  # TST-001: flagged  # noqa: E501
    pass
last = 1
"""
    assert modified_code == expected_code


def test_header_match_does_not_leak_when_the_module_ends_with_the_compound_statement():
    given_code = """
if flag(1):
    def inner():
        pass
"""
    writer = _FlagCalls()
    modified_code = rewrite(given_code, writer)
    assert (
        modified_code
        == """
if flag(1):  # TST-001: flagged  # noqa: E501
    def inner():
        pass
"""
    )
    assert writer.match_found is False
    assert rewrite("y = 2\n", writer) == "y = 2\n"


def test_module_import_collector_tracks_bindings_and_aliases():
    given_code = """
import pyspark.sql.functions as F
from pyspark.sql import functions, functions as sf
from pyspark.sql.functions import shiftLeft, sumDistinct as sd, toDegrees as toDegrees
from sklearn.preprocessing import OneHotEncoder
import numpy as np

def helper(param): pass
class Klass: pass
for loop_var in items: pass
with ctx() as with_var: pass
squares = [comp_var for comp_var in items]
if (walrus := compute()): pass
first, (second, *rest) = values
G = 1
G = 2
"""
    collector = ModuleImportCollector("pyspark", "sql", "functions")
    cst.parse_module(given_code).visit(collector)
    assert collector.module_aliases == {"F", "functions", "sf"}
    assert collector.imported_names == {
        "shiftLeft": "shiftLeft",
        "sd": "sumDistinct",
        "toDegrees": "toDegrees",
    }
    assert collector.explicit_aliases == {"sd", "toDegrees"}
    assert collector.star_import is False
    for name in [
        "OneHotEncoder",
        "np",
        "helper",
        "param",
        "Klass",
        "loop_var",
        "with_var",
        "comp_var",
        "walrus",
        "first",
        "second",
        "rest",
        "squares",
        "G",
    ]:
        assert name in collector.redefined_names, name
    assert collector.binding_counts["G"] == 2
    assert collector.binding_counts["first"] == 1
    assert collector.is_imported_name("shiftLeft", "shiftLeft")
    assert collector.is_imported_name("sd", "sumDistinct")
    assert not collector.is_imported_name("sd", "shiftLeft")
    assert collector.binds_module(cst.Name("F"))
    assert collector.binds_module(cst.parse_expression("pyspark.sql.functions"))
    assert not collector.binds_module(cst.parse_expression("pyspark.sql"))


def test_module_import_collector_rebound_alias_no_longer_binds_the_module():
    collector = ModuleImportCollector("pyspark", "sql", "functions")
    cst.parse_module("import pyspark.sql.functions as F\nF = SomethingElse()\n").visit(
        collector
    )
    assert "F" in collector.module_aliases
    assert not collector.binds_module(cst.Name("F"))
