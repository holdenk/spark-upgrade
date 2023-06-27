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
import sqlfluff

from pysparkler.base import StatementLineCommentWriter

# Migration rules for SQL statements used in PySpark from 2.x to 3.3
# https://spark.apache.org/docs/latest/sql-migration-guide.html


SPARK_SQL_DIALECT = "sparksql"
SPARK_SQL_CAST_RULE = "SPARKSQLCAST_L001"


class SqlStatementUpgradeAndCommentWriter(StatementLineCommentWriter):
    """This migration rule uses the Sqlfluff plugin written in the adjacent SQL module and makes the best effort to
    upcast SQL statements directly being executed from within a PySpark script. However, the upgrade won't be possible
    for certain templated SQLs, and in those scenarios this tool will leave code hints for users as DIY instructions.
    """

    def __init__(
        self,
    ):
        super().__init__(
            transformer_id="PY21-33-001",
            comment="Please note, PySparkler makes a best effort to upcast SQL statements directly being executed. \
However, the upcast won't be possible for certain formatted string SQL having complex expressions within, and in those \
cases please de-template the SQL and use the Sqlfluff tooling to upcast the SQL yourself.",
        )
        self.sql_upgraded = False

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        """Check if the call is a SQL statement and try to upcast it"""
        if m.matches(
            original_node,
            m.Call(
                func=m.Attribute(
                    attr=m.Name("sql"),
                ),
                args=[
                    m.Arg(
                        value=m.OneOf(
                            m.SimpleString(),
                            m.FormattedString(),
                            m.ConcatenatedString(),
                        )
                    )
                ],
            ),
        ):
            self.match_found = True
            sql_node: cst.BaseExpression = updated_node.args[0].value
            try:
                if isinstance(sql_node, cst.SimpleString):
                    updated_sql_node = self.update_simple_string_sql(sql_node)
                elif isinstance(sql_node, cst.FormattedString):
                    updated_sql_node = self.update_formatted_string_sql(sql_node)
                else:
                    raise NotImplementedError(
                        f"Unsupported SQL expression encountered : {sql_node}"
                    )

                if self.sql_upgraded:
                    self.comment = "Spark SQL statement has been upgraded to Spark 3.3 compatible syntax."
                    self.sql_upgraded = False
                else:
                    self.comment = (
                        "Spark SQL statement has Spark 3.3 compatible syntax."
                    )

                return updated_node.with_changes(args=[cst.Arg(value=updated_sql_node)])
            except Exception:  # pylint: disable=broad-except
                self.comment = "Unable to inspect the Spark SQL statement since the formatted string SQL has complex \
expressions within. Please de-template the SQL and use the 'pysparkler upgrade-sql' CLI command to upcast the SQL \
yourself."
                self.sql_upgraded = False

        return updated_node

    def update_simple_string_sql(self, sql_node: cst.SimpleString) -> cst.SimpleString:
        sql = sql_node.evaluated_value
        updated_sql = self.do_fix(sql)
        if updated_sql != sql:
            self.sql_upgraded = True
            updated_sql_value = (
                sql_node.prefix + sql_node.quote + updated_sql + sql_node.quote
            )
            return cst.SimpleString(value=updated_sql_value)
        else:
            return sql_node

    def update_formatted_string_sql(
        self, sql_node: cst.FormattedString
    ) -> cst.FormattedString:
        # Form the raw SQL string by concatenating all the parts
        sql = ""
        for part in sql_node.parts:
            if isinstance(part, cst.FormattedStringText):
                sql += part.value
            elif isinstance(part, cst.FormattedStringExpression) and isinstance(
                part.expression, cst.Name
            ):
                sql += (
                    part.whitespace_before_expression.value
                    + "{"
                    + part.expression.value
                    + "}"
                    + part.whitespace_after_expression.value
                )
            else:
                raise NotImplementedError(
                    f"Unsupported formatted string expression encountered : {part}"
                )

        updated_sql = self.do_fix(sql)
        if updated_sql != sql:
            self.sql_upgraded = True
            updated_sql_value = (
                sql_node.prefix + sql_node.quote + updated_sql + sql_node.quote
            )
            return cst.parse_expression(updated_sql_value)
        else:
            return sql_node

    @staticmethod
    def do_fix(sql: str) -> str:
        return sqlfluff.fix(
            sql,
            dialect=SPARK_SQL_DIALECT,
            rules=[SPARK_SQL_CAST_RULE],
            fix_even_unparsable=True,
        )

    @staticmethod
    def do_parse(sql: str) -> dict[str, Any]:
        return sqlfluff.parse(
            sql,
            dialect=SPARK_SQL_DIALECT,
        )


def sql_21_to_33_transformers() -> list[cst.CSTTransformer]:
    """Return a list of transformers for SQL 2.1 to 3.3 migration guide"""
    return [
        SqlStatementUpgradeAndCommentWriter(),
    ]
