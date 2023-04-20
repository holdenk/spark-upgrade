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
from tempfile import NamedTemporaryFile

from click.testing import CliRunner

from pysparkler.cli import run
from tests.conftest import absolute_path


def test_upgrade_cli():
    with NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as output_file:
        runner = CliRunner()
        result = runner.invoke(
            cli=run,
            args=[
                "--config-yaml",
                absolute_path("tests/sample/config.yaml"),
                "upgrade",
                "--input-file",
                absolute_path("tests/sample/input_pyspark.py"),
                "--output-file",
                output_file.name,
            ],
        )

        print(result.output)
        assert result.exit_code == 0  # Check exit code

    with open(output_file.name, encoding="utf-8") as f:
        modified_code = f.read()
        assert "A new comment" in modified_code

    # Clean up and delete the temporary file
    output_file.close()
