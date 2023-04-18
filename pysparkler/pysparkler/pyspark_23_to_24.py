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

from pysparkler.base import StatementLineCommentWriter


class ToPandasAllowsFallbackOnArrowOptimization(StatementLineCommentWriter):
    """In PySpark 2.4, when Arrow optimization is enabled, previously toPandas just failed when Arrow optimization is
    unable to be used whereas createDataFrame from Pandas DataFrame allowed the fallback to non-optimization. Now, both
    toPandas and createDataFrame from Pandas DataFrame allow the fallback by default, which can be switched off by
    spark.sql.execution.arrow.fallback.enabled.
    """

    def __init__(
        self,
        pyspark_version: str = "2.4",
    ):
        super().__init__(
            transformer_id="PY23-24-001",
            comment=f"As of PySpark {pyspark_version} toPandas() allows fallback to non-optimization by default when \
Arrow optimization is unable to be used. This can be switched off by spark.sql.execution.arrow.fallback.enabled",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check if toPandas is being called"""
        if m.matches(node, m.Call(func=m.Attribute(attr=m.Name("toPandas")))):
            self.match_found = True


def pyspark_23_to_24_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for PySpark 2.3 to 2.4 migration guide"""
    return [ToPandasAllowsFallbackOnArrowOptimization()]
