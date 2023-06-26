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
However, the upgrade won't be possible for certain templated SQLs, and in those scenarios please de-template the SQL \
and use the Sqlfluff tooling to upcast the SQL yourself.",
        )

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        """Check if the call is a SQL statement and try to upcast it"""
        print(f"******** Call node\n{original_node}")
        if m.matches(
            updated_node,
            m.Call(
                func=m.Attribute(
                    attr=m.Name("sql"),
                ),
                args=[
                    m.Arg(
                        value=m.SimpleString(),
                    )
                ],
            ),
        ):
            print(f"******** Match found\n{original_node}")
            self.match_found = True
            sql_node: cst.SimpleString = updated_node.args[0].value
            sql = sql_node.evaluated_value
            try:
                updated_sql = self.do_fix(sql)
                if updated_sql != sql:
                    updated_sql_value = (
                        sql_node.prefix + sql_node.quote + updated_sql + sql_node.quote
                    )
                    changes = updated_node.with_changes(
                        args=[cst.Arg(value=cst.SimpleString(value=updated_sql_value))]
                    )
                    return changes
            except Exception as e:  # pylint: disable=broad-except
                print(f"Failed to parse SQL: {sql} with error: {e}")

        return updated_node

    @staticmethod
    def do_fix(sql: str) -> str:
        return sqlfluff.fix(
            sql,
            dialect=SPARK_SQL_DIALECT,
            rules=[SPARK_SQL_CAST_RULE],
            fix_even_unparsable=True,
        )
