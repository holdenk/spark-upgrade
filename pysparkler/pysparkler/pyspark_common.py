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

# Cross-version (version-agnostic) PySpark correctness and portability lints.
#
# Unlike the pyspark_<from>_to_<to> modules, these rules are not tied to a single Spark
# release. Their transformer ids use the PYC- ("PySpark common") prefix.


class TriggerOnceDeprecated(StatementLineCommentWriter):
    """``Trigger.Once`` / ``.trigger(once=True)`` was deprecated in Spark 3.3 in favor of
    ``Trigger.AvailableNow`` / ``.trigger(availableNow=True)``, which processes all available data
    in (potentially) multiple micro-batches with rate-limiting honored.
    """

    def __init__(self) -> None:
        super().__init__(
            transformer_id="PYC-001",
            comment="trigger(once=True) (Trigger.Once) is deprecated since Spark 3.3. Use \
trigger(availableNow=True) (Trigger.AvailableNow) instead, which honors rate limits across multiple micro-batches.",
        )

    def visit_Call(self, node: cst.Call) -> None:
        """Check for a streaming .trigger(once=...) call"""
        if m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.Name("trigger")),
                args=[
                    m.ZeroOrMore(),
                    m.Arg(keyword=m.Name("once")),
                    m.ZeroOrMore(),
                ],
            ),
        ):
            self.match_found = True


class BuiltinFunctionShadowing(StatementLineCommentWriter):
    """Importing functions such as ``max``, ``min``, ``sum``, ``round``, ``abs`` directly from
    ``pyspark.sql.functions`` (or via a wildcard import) shadows the Python builtins of the same
    name. Calling the shadowed name with builtin-only arguments (for example ``max(xs, key=...)`` or
    ``sorted(...)``) then fails or silently changes behavior.
    """

    # Names exported by pyspark.sql.functions that collide with commonly-used Python builtins.
    shadowed_builtins = frozenset(
        {"max", "min", "sum", "round", "abs", "sorted", "filter", "map"}
    )

    def __init__(self) -> None:
        super().__init__(
            transformer_id="PYC-002",
            comment="This import shadows a Python builtin of the same name from pyspark.sql.functions. \
Calling it with builtin-only arguments (e.g. key=) will fail. Prefer importing the module, e.g. \
`from pyspark.sql import functions as F`, or alias the function on import.",
        )

    def _is_functions_module(self, node: cst.ImportFrom) -> bool:
        return m.matches(
            node,
            m.ImportFrom(
                module=m.Attribute(
                    value=m.Attribute(value=m.Name("pyspark"), attr=m.Name("sql")),
                    attr=m.Name("functions"),
                )
            ),
        )

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        """Check for builtin-shadowing imports from pyspark.sql.functions"""
        if not self._is_functions_module(node):
            return
        if isinstance(node.names, cst.ImportStar):
            # A wildcard import pulls in every shadowing name.
            self.match_found = True
            return
        for alias in node.names:
            # Only flag unaliased imports; `import max as spark_max` is safe.
            if (
                alias.asname is None
                and isinstance(alias.name, cst.Name)
                and alias.name.value in self.shadowed_builtins
            ):
                self.match_found = True
                return


class RemovedOrRenamedConfig(StatementLineCommentWriter):
    """Several Spark configurations were renamed or removed in Spark 4.x. When a known
    removed/renamed config is passed to ``.set(...)`` / ``.config(...)`` (e.g.
    ``spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", ...)``), suggest the
    replacement so the setting keeps taking effect after the upgrade.
    """

    # Old config name -> replacement config name (Spark 4.0 / 4.1).
    renamed_configs = {
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "spark.sql.parquet.int96RebaseModeInWrite",
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "spark.sql.parquet.datetimeRebaseModeInWrite",
        "spark.sql.legacy.parquet.int96RebaseModeInRead": "spark.sql.parquet.int96RebaseModeInRead",
        "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "spark.sql.parquet.datetimeRebaseModeInRead",
        "spark.sql.legacy.avro.datetimeRebaseModeInWrite": "spark.sql.avro.datetimeRebaseModeInWrite",
        "spark.sql.legacy.avro.datetimeRebaseModeInRead": "spark.sql.avro.datetimeRebaseModeInRead",
        "spark.shuffle.unsafe.file.output.buffer": "spark.shuffle.localDisk.file.output.buffer",
    }

    def __init__(self) -> None:
        super().__init__(transformer_id="PYC-003", comment="")

    def visit_Call(self, node: cst.Call) -> None:
        """Check for set()/config() calls passing a removed or renamed Spark config key"""
        if not m.matches(
            node,
            m.Call(
                func=m.Attribute(attr=m.OneOf(m.Name("set"), m.Name("config"))),
                args=[m.Arg(value=m.SimpleString()), m.ZeroOrMore()],
            ),
        ):
            return
        config = node.args[0].value.value.strip("\"'")
        if config in self.renamed_configs:
            self.comment = (
                f"{config} was renamed in Spark 4.x; it no longer takes effect. "
                f"Use {self.renamed_configs[config]} instead."
            )
            self.match_found = True
        elif ".blacklist." in config or config.endswith(".blacklist"):
            self.comment = (
                f"{config} uses the deprecated 'blacklist' naming, which Spark 4.1 ignores. "
                "Use the corresponding 'excludeOnFailure' configuration name (Spark 3.1.0+) instead."
            )
            self.match_found = True


def pyspark_common_transformers() -> list[cst.CSTTransformer]:
    """Return a list of version-agnostic PySpark lint transformers"""
    return [
        TriggerOnceDeprecated(),
        BuiltinFunctionShadowing(),
        RemovedOrRenamedConfig(),
    ]
