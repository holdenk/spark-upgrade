"""Tests for Rule_BANGNOT_L008.

A bare ``!`` cannot be parsed by sqlfluff's sparksql dialect, so the YAML rule harness
(which fails on any parse error) cannot be used. These tests go through the simple API
with ``fix_even_unparsable=True`` instead, which is how the rule is meant to be applied.
"""
import pytest

import sqlfluff

RULE = "BANGNOT_L008"


def _fix(sql: str) -> str:
    return sqlfluff.fix(sql, dialect="sparksql", rules=[RULE], fix_even_unparsable=True)


def _lint_codes(sql: str) -> list:
    return [
        violation["code"]
        for violation in sqlfluff.lint(sql, dialect="sparksql", rules=[RULE])
    ]


@pytest.mark.parametrize(
    "before, after",
    [
        (
            "SELECT * FROM t WHERE a ! IN (1,2)",
            "SELECT * FROM t WHERE a NOT IN (1,2)",
        ),
        (
            "SELECT * FROM t WHERE b ! BETWEEN 1 AND 2",
            "SELECT * FROM t WHERE b NOT BETWEEN 1 AND 2",
        ),
        (
            "SELECT * FROM t WHERE c IS ! NULL",
            "SELECT * FROM t WHERE c IS NOT NULL",
        ),
        (
            "SELECT * FROM t WHERE d ! LIKE 'x%'",
            "SELECT * FROM t WHERE d NOT LIKE 'x%'",
        ),
        (
            "SELECT * FROM t WHERE ! EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
            "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
        ),
        (
            "CREATE TABLE t (a INT ! NULL)",
            "CREATE TABLE t (a INT NOT NULL)",
        ),
        # No whitespace after the bang: one must be inserted.
        (
            "SELECT * FROM t WHERE a !IN (1,2)",
            "SELECT * FROM t WHERE a NOT IN (1,2)",
        ),
        # Several bangs in one unparsable section are all rewritten.
        (
            "SELECT * FROM t WHERE a ! IN (1,2) AND c IS ! NULL",
            "SELECT * FROM t WHERE a NOT IN (1,2) AND c IS NOT NULL",
        ),
    ],
)
def test_bang_rewritten_to_not(before, after):
    assert _fix(before) == after
    assert RULE in _lint_codes(before)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM t WHERE b != 3",
        "SELECT * FROM t WHERE b ! = 3",
        "SELECT * FROM t WHERE c <> 4",
        "SELECT * FROM t WHERE d = 'x!y'",
        "SELECT * FROM t WHERE `we!rd` = 1",
        "SELECT * FROM t WHERE a = 1 -- really!\n",
    ],
)
def test_untouched(sql):
    assert _fix(sql) == sql
    assert RULE not in _lint_codes(sql)


def test_bang_equals_inside_unparsable_section_untouched():
    """``!=`` next to a real bang lands in the same unparsable section and must survive."""
    before = "SELECT * FROM t WHERE a ! IN (1,2) AND b != 3 AND d = 'x!y' AND `we!rd` = 1"
    after = "SELECT * FROM t WHERE a NOT IN (1,2) AND b != 3 AND d = 'x!y' AND `we!rd` = 1"
    assert _fix(before) == after
