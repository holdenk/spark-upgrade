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

from pysparkler.pyspark_24_to_30 import visit_pyspark_24_to_30


class PySparkler:
    """Main class for PySparkler"""

    def __init__(
        self, from_pyspark: str = "2.4", to_pyspark: str = "3.0", dry_run: bool = False
    ):
        self.from_pyspark = from_pyspark
        self.to_pyspark = to_pyspark
        self.dry_run = dry_run

    def upgrade(self, input_file: str, output_file: str | None = None) -> str:
        """Upgrade the PySparkler file to the latest version and provides comments as hints for manual changes"""
        # Parse the PySpark script written in version 2.4
        with open(input_file, encoding="utf-8") as f:
            original_code = f.read()
        original_tree = cst.parse_module(original_code)

        # Apply the re-writer to the AST
        modified_tree = visit_pyspark_24_to_30(original_tree)

        if not self.dry_run:
            if output_file:
                # Write the modified AST to the output file location
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(modified_tree.code)
            else:
                # Re-write the modified AST back to the input file
                with open(input_file, "w", encoding="utf-8") as f:
                    f.write(modified_tree.code)

        # Verify the AST was modified
        return modified_tree.code
