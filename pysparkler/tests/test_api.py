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
import nbformat

from pysparkler.api import PySparkler
from tests.conftest import absolute_path


def test_upgrade_pyspark_python_script():
    modified_code = PySparkler(dry_run=True).upgrade_script(
        input_file=absolute_path("tests/sample/input_pyspark.py")
    )

    with open(
        file=absolute_path("tests/sample/output_pyspark.py"), encoding="utf-8"
    ) as f:
        expected_code = f.read()

    assert modified_code == expected_code


def test_upgrade_pyspark_jupyter_notebook():
    modified_code = PySparkler(dry_run=True).upgrade_notebook(
        input_file=absolute_path("tests/sample/InputPySparkNotebook.ipynb"),
        output_kernel_name="spark33-python3-venv",
    )

    with open(
        file=absolute_path("tests/sample/OutputPySparkNotebook.ipynb"), encoding="utf-8"
    ) as f:
        expected_code = f.read()

    assert nbformat.reads(
        modified_code, as_version=nbformat.NO_CONVERT
    ) == nbformat.reads(expected_code, as_version=nbformat.NO_CONVERT)


def test_disable_transformers_are_filtered_out():
    transformer_id = "PY24-30-001"
    given_overrides = {
        transformer_id: {"enabled": "False"},
    }
    transformers = PySparkler(**given_overrides).transformers

    assert transformer_id not in [t.transformer_id for t in transformers]


def test_transformer_override_comments_are_taking_effect():
    transformer_id = "PY24-30-001"
    overriden_comment = "A new comment"
    given_overrides = {
        transformer_id: {"comment": overriden_comment},
    }

    modified_code = PySparkler(dry_run=True, **given_overrides).upgrade_script(
        input_file=absolute_path("tests/sample/input_pyspark.py")
    )

    assert overriden_comment in modified_code
