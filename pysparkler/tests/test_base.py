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

from pysparkler.base import StatementLineCommentWriter
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
