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

from pysparkler.base import RequiredDependencyVersionCommentWriter


class RequiredPandasVersionCommentWriter(RequiredDependencyVersionCommentWriter):
    """In PySpark 2.3, now we need Pandas 0.19.2 or upper if you want to use Pandas related functionalities, such as
    toPandas, createDataFrame from Pandas DataFrame, etc."""

    def __init__(
        self,
        pyspark_version: str = "2.3",
        required_dependency_name: str = "pandas",
        required_dependency_version: str = "0.19.2",
    ):
        super().__init__(
            transformer_id="PY22-23-001",
            pyspark_version=pyspark_version,
            required_dependency_name=required_dependency_name,
            required_dependency_version=required_dependency_version,
        )


def pyspark_22_to_23_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 2.2 to 2.3 migration guide"""
    return [RequiredPandasVersionCommentWriter()]
