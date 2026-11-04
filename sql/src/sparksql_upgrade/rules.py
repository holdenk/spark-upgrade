"""Custom Spark SQL upgrade rules."""

import os.path
import re
from typing import List, Optional


from sqlfluff.core.config import ConfigLoader
from sqlfluff.core.parser import (
    KeywordSegment,
    SymbolSegment,
    WhitespaceSegment,
)
from sqlfluff.core.parser.segments.raw import CodeSegment
from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.rules import (
    BaseRule,
    LintFix,
    LintResult,
    RuleContext,
)
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.utils.functional import FunctionalContext, sp


@hookimpl
def load_default_config() -> dict:
    """Loads the default configuration for the plugin."""
    return ConfigLoader.get_global().load_config_file(
        file_dir=os.path.dirname(__file__),
        file_name="plugin_default_config.cfg",
    )


@hookimpl
def get_configs_info() -> dict:
    """Get rule config validations and descriptions."""
    return {
        "forbidden_columns": {"definition": "A list of column to forbid"},
    }


class Rule_SPARKSQLCAST_L001(BaseRule):
    """Spark 3.0 cast as int on strings will fail.

    Instead use the int() function.

    **Spark 2.4**

    Cast a string to an int

    .. code-block:: sql

        SELECT cast(foocount as int)
        FROM foo

    **Best practice**

    Use the int() function.

    .. code-block:: sql

        SELECT int(foocount)
        FROM foo
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check integer casts."""
        functional_context = FunctionalContext(context)
        children = functional_context.segment.children()
        function_name_id_seg = (
            children.first(sp.is_type("function_name"))
            .children()
            .first(sp.is_type("function_name_identifier"))[0]
        )
        raw_function_name = function_name_id_seg.raw.upper().strip()
        function_name = raw_function_name.upper().strip()
        bracketed_segments = children.first(sp.is_type("bracketed"))
        if not bracketed_segments:
            return None
        bracketed = bracketed_segments[0]

        # Is this a cast function call
        if function_name == "CAST":
            print("Found cast function!")
            data_type_info = bracketed.get_child("data_type").raw.upper().strip()
            if data_type_info == "INT":
                # Here we know we have a possible one
                expr = bracketed.get_child("expression")
                print(f"Found expr {expr} - {expr.raw}")
                # Replace cast(X as int) with int(X) TODO
                return LintResult(
                    anchor=context.segment,
                    fixes=[
                        LintFix.replace(
                            function_name_id_seg,
                            [function_name_id_seg.edit(f"int({expr.raw})")],
                        ),
                        LintFix.delete(
                            bracketed,
                        ),
                    ],
                )

        return None


class Rule_FORMATSTRONEINDEX_L004(BaseRule):
    """Spark 3.3 Format strings are one indexed.


    Previously on JDK8 format strings were still one indexed, but zero was treated as one.
    On JDK17 an exception was thrown.
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check for invalid format strs"""
        functional_context = FunctionalContext(context)
        children = functional_context.segment.children()
        function_name_id_seg = (
            children.first(sp.is_type("function_name"))
            .children()
            .first(sp.is_type("function_name_identifier"))[0]
        )
        raw_function_name = function_name_id_seg.raw.upper().strip()
        function_name = raw_function_name.upper().strip()
        bracketed_segments = children.first(sp.is_type("bracketed"))
        if not bracketed_segments:
            return None
        bracketed = bracketed_segments[0]

        # Is this a cast function call
        if function_name == "FORMAT_STRING":
            print("Found format string function!")
            format_str_seg = bracketed.get_child("expression").get_child(
                "quoted_literal"
            )
            format_str = format_str_seg.raw
            # If we don't use the bad sequence just return right away.
            if "%0$" not in format_str:
                return None
            else:
                # Replace %0$ with %1$
                return LintResult(
                    anchor=context.segment,
                    fixes=[
                        LintFix.replace(
                            format_str_seg,
                            [format_str_seg.edit(format_str.replace("%0$", "%1$"))],
                        ),
                    ],
                )

        return None


class Rule_NOCHARS_L003(BaseRule):
    """Spark 3.0 No longer supports CHAR type in non-Hive tables.

    In Spark 2.4 the CHAR type was treated as a String type anyways so we'll
    just rewrite all CHAR types to String types.
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"primitive_type"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check for char types."""
        type_name = context.segment.raw.lower()
        if type_name.startswith("char"):
            return LintResult(
                anchor=context.segment,
                description=f"char type found ({type_name}) in {context} see "
                "https://spark.apache.org/docs/3.0.0/sql-migration-guide.html for migration"
                "advice. In Spark 2.4 on non-Hive tables these were treated as strings so "
                "rewritting to string.",
                fixes=[LintFix.replace(context.segment, [CodeSegment(raw="string")])],
            )
        else:
            return None


class Rule_RESERVEDROPERTIES_L002(BaseRule):
    """Spark 3.0 Reserves some table properties

    You can no longer set the provider, location, or owner property.
    For provider this is replaced with USING and location with LOCATION in the create.
    Sets after creation are not supported.
    Owner property is infered from running user.
    """

    groups = ("all",)
    # TODO -- Also look at SET calls once we fix SET DBPROPS in SQLFLUFF grammar.
    crawl_behaviour = SegmentSeekerCrawler({"property_name_identifier"})
    reserved = {"provider", "location", "owner"}
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check for reserved properties being configured."""
        functional_context = FunctionalContext(context)
        property_name_segment = context.segment
        property_name = (
            property_name_segment.raw.lower()
            .strip()
            .lstrip('"')
            .rstrip('"')
            .lstrip("'")
            .rstrip("'")
        )
        print(f'Called with context {context} with "{property_name}"')
        if property_name not in self.reserved:
            print(f"Property: {property_name} is *ok*")
            return None
        else:
            # Reserved property found, lets check and see if we are in a "CREATE" which we can fix
            # or if we are in an "ALTER" which we can not automatically fix.
            create_or_alter_segment = context.parent_stack[-2]
            print(f"{dir(create_or_alter_segment)}")
            if create_or_alter_segment.is_type(
                "alter_database_statement"
            ) or create_or_alter_segment.is_type("alter_table_statement"):
                return LintResult(
                    anchor=context.segment,
                    description=f"Reserved table/db property {property_name} found in alter "
                    " statement see "
                    "https://spark.apache.org/docs/3.0.0/sql-migration-guide.html for migration "
                    " advice."
                    "In Spark 2.4 these alter statements were (effectively) ignored so you can "
                    " likely delete it, automatically "
                    f'rewritten to "legacy_{property_name}".',
                    fixes=[
                        LintFix.replace(
                            property_name_segment,
                            [
                                property_name_segment.get_child(
                                    "quoted_identifier"
                                ).edit(f'"legacy_{property_name}"')
                            ],
                        )
                    ],
                )
            # Ok we know it's a create statement since it is not an alter :)
            parent_segment = context.parent_stack[-1]
            # Now we want to get the segments that are "bad" (e.g. we want to delete) and that is
            # everything from this segment up until either a comma segment or a endbracket segment.
            segments_to_remove = []
            edits = []
            property_value = None
            siblings_post = functional_context.siblings_post
            segments_to_remove = [property_name_segment]
            for segment in siblings_post:
                # We want to keep the end bracket so check for it before adding to the list
                if segment.is_type("end_bracket"):
                    break
                segments_to_remove.append(segment)
                if segment.is_type("quoted_literal"):
                    property_value = segment.raw.strip().lstrip('"').rstrip('"')
                print(f"{segment} - {segment.get_type()}")
                # We want to drop the comma so we do the check _after_ the ops
                if segment.is_type("comma"):
                    break
            if len(parent_segment.get_children("property_name_identifier")) == 1:
                # If there are no other properties besides the property we are going to delete then
                # we need to drop the entire properties part to do this correctly, but "for now"
                # as a hack we will just edit the property name to prepend "legacy_"
                segments_to_remove = []
                edits = [
                    LintFix.replace(
                        property_name_segment,
                        [
                            property_name_segment.get_child("quoted_identifier").edit(
                                f'"legacy_{property_name}"'
                            )
                        ],
                    )
                ]
            deletes = map(lambda t: LintFix.delete(t), segments_to_remove)
            new_statement = None
            if property_name == "provider":
                new_segment = CodeSegment(raw=f" USING {property_value}")
                print(functional_context.parent_stack)
                create_table_segment = functional_context.parent_stack[-2]
                # We want to insert after the first bracketed segment containing column_definition
                # but if there are no column definitions we instead insert after the table
                # identifier.
                first_bracketed_segment = create_table_segment.get_child("bracketed")
                print(dir(first_bracketed_segment))
                if (
                    "column_definition"
                    in first_bracketed_segment.direct_descendant_type_set
                ):
                    new_statement = LintFix.create_after(
                        first_bracketed_segment,
                        [new_segment],
                    )
                else:
                    new_statement = LintFix.create_after(
                        create_table_segment.get_child("table_reference"),
                        [new_segment],
                    )
            elif property_name == "location":
                create_segment = functional_context.parent_stack[-2]
                # We want to insert after the database reference (and comment if present) or before
                # "TBLPROPERTIES" depending.
                if "database_reference" in create_segment.direct_descendant_type_set:
                    new_statement = LintFix.create_after(
                        create_segment.get_child("database_reference"),
                        [CodeSegment(raw=f' LOCATION "{property_value}"')],
                    )
                else:
                    keywords = create_segment.get_children("keyword")
                    tbl_properties_ref = next(
                        iter(filter(lambda s: s.raw_upper == "TBLPROPERTIES", keywords))
                    )
                    new_statement = LintFix.create_before(
                        tbl_properties_ref,
                        [CodeSegment(raw=f'LOCATION "{property_value}" ')],
                    )
            else:
                # For "owner" property we don't have an easy work around so instead just raise
                # a lint error.
                return LintResult(
                    anchor=context.segment,
                    description=f"Reserved table/db property {property_name} found see "
                    "https://spark.apache.org/docs/3.0.0/sql-migration-guide.html for "
                    "migration advice.",
                    fixes=None,
                )
            fixes = list(edits) + list(deletes) + [new_statement]
            return LintResult(
                anchor=context.segment,
                description="Reserved table property {property_name} found.",
                fixes=fixes,
            )
            # TODO - Make a rewrite rule.


class Rule_SPARKSQL_L004(BaseRule):
    """Spark 3.0 extract second field returns DecimalType

    Since Spark 3.0, when using EXTRACT expression to extract the
    second field from date/timestamp values, the result will be a
    DecimalType(8, 6) value with 2 digits for second part, and 6 digits
    for the fractional part with microsecond precision. e.g.
    extract(second from to_timestamp('2019-09-20 10:10:10.1')) results
    10.100000. In Spark version 2.4 and earlier, it returns an IntegerType
    value and the result for the former example is 10.
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        functional_context = FunctionalContext(context)
        children = functional_context.segment.children()
        function_name_id_seg = (
            children.first(sp.is_type("function_name"))
            .children()
            .first(sp.is_type("function_name_identifier"))[0]
        )
        raw_function_name = function_name_id_seg.raw.upper().strip()
        function_name = raw_function_name.upper().strip()
        bracketed_segments = children.first(sp.is_type("bracketed"))
        if not bracketed_segments:
            return None
        bracketed = bracketed_segments[0]

        if function_name == "EXTRACT":
            print("Found extract function!")

            parent_stack_reversed = functional_context.parent_stack.reversed()
            if (
                parent_stack_reversed[0].get_type() == "expression" and
                parent_stack_reversed[1].get_type() == "bracketed" and
                parent_stack_reversed[2].get_type() == "function"
            ):
                if parent_stack_reversed[2].segments[0].raw.upper().strip() == "INT":
                    return None

            date_part_into = bracketed.get_child("date_part").raw.upper().strip()
            if date_part_into == "SECOND":
                edits = [
                    KeywordSegment("int"),
                    SymbolSegment("(", type="start_bracket"),
                    context.segment,
                    SymbolSegment(")", type="end_bracket"),
                ]
                return LintResult(
                    anchor=context.segment,
                    fixes=[
                        LintFix.replace(context.segment, edits),
                    ],
                )

        return None


class Rule_GLOBALTEMPVIEW_L006(BaseRule):
    """Global temporary views are not portable across all Spark runtimes.

    ``CREATE GLOBAL TEMPORARY VIEW`` ties the view to the cross-session ``global_temp``
    database and the lifetime of the Spark application. Some runtimes (for example Spark
    Connect / Databricks serverless) do not support global temporary views. There is no
    safe automatic rewrite, so this rule only warns: depending on intent, switch to a
    regular session-scoped view (``CREATE TEMPORARY VIEW``) or a persistent table/view.

    **Flagged**

    .. code-block:: sql

        CREATE GLOBAL TEMPORARY VIEW v AS SELECT 1
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"create_view_statement"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Warn when a view is created as GLOBAL TEMPORARY."""
        keywords = [
            segment.raw_upper
            for segment in context.segment.segments
            if segment.is_type("keyword")
        ]
        if "GLOBAL" in keywords:
            return LintResult(
                anchor=context.segment,
                description="GLOBAL TEMPORARY VIEW is not supported on all Spark runtimes "
                "(e.g. Spark Connect / serverless). Use a regular TEMPORARY VIEW or a "
                "persistent table/view instead. See "
                "https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-view.html",
            )
        return None


class Rule_SPARKSQL_L005(BaseRule):
    """Spark 3.0 approx_percentile only accepts int type

    In Spark 3.0, the function percentile_approx and its
    alias approx_percentile only accept integral value with
    range in [1, 2147483647] as its 3rd argument accuracy,
    fractional and string types are disallowed, for example,
    percentile_approx(10.0, 0.2, 1.8D) causes AnalysisException.
    In Spark version 2.4 and below, if accuracy is fractional or
    string value, it is coerced to an int value, percentile_approx(10.0, 0.2, 1.8D)
    is operated as percentile_approx(10.0, 0.2, 1) which results in 10.0.
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        functional_context = FunctionalContext(context)
        children = functional_context.segment.children()
        function_name_id_seg = (
            children.first(sp.is_type("function_name"))
            .children()
            .first(sp.is_type("function_name_identifier"))[0]
        )
        raw_function_name = function_name_id_seg.raw.upper().strip()
        function_name = raw_function_name.upper().strip()
        bracketed_segments = children.first(sp.is_type("bracketed"))

        if function_name == "APPROX_PERCENTILE" or function_name == "PERCENTILE_APPROX":
            print(f"Found approx function! {function_name}")

            expression_count = 0
            expression_segment = None
            # Find "middle" of the approx_percentile(bloop) (e.g. bloop)
            for segment in bracketed_segments.children().iterate_segments(
                sp.is_type("expression")
            ):
                expression_count += 1
                if expression_count == 3:
                    expression_segment = segment

            if expression_segment is not None:
                expression_child = expression_segment.children().first()
                # cast can either be a keyword or a function depending on if were iterating on
                # parsed on updated code.
                if expression_child[0].type == "keyword":
                    if expression_child.child[0].raw == "cast":
                        return None
                elif expression_child[0].type == "function":
                    function_name_id_seg = (
                        expression_child.children()
                        .first(sp.is_type("function_name"))
                        .children()
                        .first(sp.is_type("function_name_identifier"))[0]
                    )
                    raw_function_name = function_name_id_seg.raw.upper().strip()
                    function_name = raw_function_name.upper().strip()
                    print(function_name)
                    # If we see a cast then we know this was already fixed.
                    if function_name == "CAST":
                        return None
                expression_child = expression_child[0]
                edits = [
                    KeywordSegment("cast"),
                    SymbolSegment("(", type="start_bracket"),
                    expression_child,
                    WhitespaceSegment(),
                    KeywordSegment("as"),
                    WhitespaceSegment(),
                    KeywordSegment("int"),
                    SymbolSegment(")", type="end_bracket"),
                ]
                return LintResult(
                    anchor=context.segment,
                    fixes=[
                        LintFix.replace(expression_child, edits),
                    ],
                )

        return None


class Rule_CREATETABLENOPROVIDER_L007(BaseRule):
    """Spark 4.0 CREATE TABLE without a provider creates a data-source table.

    Up to Spark 3.5 ``CREATE TABLE t (...)`` with no ``USING``, ``STORED AS`` or
    ``ROW FORMAT`` clause created a Hive SerDe table (``hive.default.fileformat``,
    normally TEXTFILE) because ``spark.sql.legacy.createHiveTableByDefault`` defaulted
    to true. Spark 4.0 flips that default, so the same statement now creates a
    ``spark.sql.sources.default`` (parquet) data-source table: a different on-disk
    format with different Hive / external-reader interoperability.

    There is no safe automatic rewrite (keeping TEXTFILE vs adopting parquet is a
    user decision), so this rule only warns.

    **Flagged**

    .. code-block:: sql

        CREATE TABLE t (a INT, b STRING)
        CREATE TABLE t AS SELECT * FROM src

    **Not flagged**

    .. code-block:: sql

        CREATE TABLE t (a INT) USING parquet
        CREATE TABLE t (a INT) STORED AS textfile
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"create_table_statement"})
    is_fix_compatible = False
    # Keywords (direct children) that mean the provider / SerDe is already pinned or that
    # the statement is not affected (LIKE copies the provider, TEMPORARY and OR REPLACE
    # are v2-only forms with no 3.5 -> 4.0 change). STORED covers STORED AS and STORED BY.
    _provider_keywords = {"STORED", "LIKE", "TEMPORARY", "REPLACE"}

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Warn when a CREATE TABLE has no USING / STORED AS / ROW FORMAT clause."""
        segment = context.segment
        for child in segment.segments:
            if child.is_type("using_clause") or child.is_type("row_format_clause"):
                return None
            if child.is_type("keyword") and child.raw_upper in self._provider_keywords:
                return None
        return LintResult(
            anchor=segment,
            description="This CREATE TABLE has no USING / STORED AS / ROW FORMAT clause. "
            "Spark 3.5 (spark.sql.legacy.createHiveTableByDefault=true) created a Hive SerDe "
            "table (default TEXTFILE); Spark 4.0 defaults that config to false and creates a "
            "spark.sql.sources.default (parquet) data-source table, changing on-disk format "
            "and Hive/external-reader interoperability. Pin the provider explicitly: add "
            "STORED AS TEXTFILE (or the Hive format you rely on) to keep the old behaviour, "
            "or USING parquet to adopt the new one; alternatively set "
            "spark.sql.legacy.createHiveTableByDefault=true (or env "
            "SPARK_SQL_LEGACY_CREATE_HIVE_TABLE=true). See "
            "https://spark.apache.org/docs/latest/sql-migration-guide.html"
            "#upgrading-from-spark-sql-35-to-40",
        )


class Rule_BANGNOT_L008(BaseRule):
    """Spark 4.0 only accepts ``!`` as a prefix NOT.

    Spark 3.x treated ``!`` as a full synonym of ``NOT`` everywhere, so Hive-style
    ``a ! IN (...)``, ``a ! BETWEEN``, ``c IS ! NULL``, ``d ! LIKE`` and
    ``a INT ! NULL`` were accepted. Spark 4.0 raises a syntax error for all of
    these unless ``spark.sql.legacy.bangEqualsNot=true``. Replacing every
    standalone ``!`` token with ``NOT`` is equivalent on every Spark version
    (``!=`` is a separate token and is never touched).

    **Important**: sqlfluff's sparksql dialect cannot parse a bare ``!`` either, so
    the plugin only ever sees it inside ``unparsable`` segments. The rewrite is
    therefore only applied when fixing even unparsable sections: pass
    ``--FIX-EVEN-UNPARSABLE`` to ``sqlfluff fix`` on the command line (plain
    ``sqlfluff fix`` refuses to write files that have parse errors) or
    ``fix_even_unparsable=True`` to ``sqlfluff.fix()`` in the Python API.

    **Spark 3.x**

    .. code-block:: sql

        SELECT * FROM t WHERE a ! IN (1, 2) AND c IS ! NULL

    **Spark 4.0**

    .. code-block:: sql

        SELECT * FROM t WHERE a NOT IN (1, 2) AND c IS NOT NULL
    """

    groups = ("all",)
    # allow_recurse=False: nested unparsable segments are covered by the outermost one
    # because we walk raw_segments, so do not visit them (and their bangs) twice.
    crawl_behaviour = SegmentSeekerCrawler(
        {"unparsable"}, works_on_unparsable=True, allow_recurse=False
    )
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> Optional[List[LintResult]]:
        """Rewrite every standalone ``!`` token inside an unparsable section to NOT."""
        raws = list(context.segment.raw_segments)
        results = []
        for idx, raw_segment in enumerate(raws):
            # String literals, quoted identifiers and comments keep their own segment types
            # inside unparsable sections, so only a bare "raw" token is ever a bang operator.
            if not raw_segment.is_type("raw") or raw_segment.raw != "!":
                continue
            following = raws[idx + 1] if idx + 1 < len(raws) else None
            if following is not None and following.raw == "=":
                # "!=" is lexed as two adjacent raw tokens inside unparsable sections.
                continue
            edits = [KeywordSegment("NOT")]
            if following is None or not following.is_type("whitespace"):
                edits.append(WhitespaceSegment())
            results.append(
                LintResult(
                    anchor=raw_segment,
                    description="Spark 4.0 only accepts '!' as a prefix NOT; '!' before "
                    "IN/BETWEEN/LIKE/NULL/EXISTS now raises a syntax error. Rewritten to NOT "
                    "(equivalent on all Spark versions). Alternatively set "
                    "spark.sql.legacy.bangEqualsNot=true.",
                    fixes=[LintFix.replace(raw_segment, edits)],
                )
            )
        return results or None


def _function_name_and_bracketed(context: RuleContext):
    """Return (FUNCTION_NAME, bracketed segment) for a ``function`` segment, or None."""
    functional_context = FunctionalContext(context)
    children = functional_context.segment.children()
    function_name_ids = (
        children.first(sp.is_type("function_name"))
        .children()
        .first(sp.is_type("function_name_identifier"))
    )
    bracketed_segments = children.first(sp.is_type("bracketed"))
    if not function_name_ids or not bracketed_segments:
        return None
    return function_name_ids[0].raw.upper().strip(), bracketed_segments[0]


def _expression_arguments(bracketed) -> list:
    """Return the direct ``expression`` children of a function's bracketed segment."""
    return [child for child in bracketed.segments if child.is_type("expression")]


def _single_quoted_literal(expression):
    """Return the quoted_literal segment if the expression is exactly one literal."""
    code_children = [child for child in expression.segments if child.is_code]
    if len(code_children) == 1 and code_children[0].is_type("quoted_literal"):
        return code_children[0]
    return None


def _literal_value(quoted_literal) -> str:
    """Strip the outer quotes of a SQL string literal and unescape doubled quotes."""
    raw = quoted_literal.raw.strip()
    if len(raw) >= 2 and raw[0] in ("'", '"') and raw[-1] == raw[0]:
        quote = raw[0]
        return raw[1:-1].replace(quote * 2, quote)
    return raw


class Rule_DATETIMEPATTERN_L009(BaseRule):
    """Spark 3.0 datetime patterns are checked by DateTimeFormatter, not SimpleDateFormat.

    Since Spark 3.0 (``spark.sql.legacy.timeParserPolicy`` != LEGACY) the pattern
    strings passed to ``date_format`` / ``from_unixtime`` (formatting) and
    ``to_date`` / ``to_timestamp`` / ``unix_timestamp`` / ``to_unix_timestamp``
    (parsing) follow https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html.
    Spark rejects at analysis time: all week-based letters (``Y W w u e c``), the
    letters ``A B n N p``, the letters ``E F q Q`` when *parsing*, and runs of five or
    more ``G M L E Q q`` or seven or more ``y``. ``F`` silently changed meaning when
    formatting (week-of-month -> aligned day-of-week-in-month).

    Only literal patterns are inspected and only the parts outside single-quoted
    sections are scanned; column-valued patterns are skipped. There is no safe
    automatic rewrite (e.g. ``Y`` and ``y`` differ around year boundaries), so this
    rule only warns.

    **Flagged**

    .. code-block:: sql

        SELECT to_date(s, 'dd/MM/YYYY'), unix_timestamp(s, 'yyyy-MM-dd EEE') FROM t

    **Not flagged**

    .. code-block:: sql

        SELECT date_format(ts, 'yyyy-MM-dd HH:mm:ss'), date_format(ts, 'EEEE') FROM t
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = False

    formatting_functions = {"DATE_FORMAT", "FROM_UNIXTIME"}
    parsing_functions = {"TO_DATE", "TO_TIMESTAMP", "UNIX_TIMESTAMP", "TO_UNIX_TIMESTAMP"}
    week_based_letters = "YWwuec"
    illegal_letters = "ABnNp"
    formatting_only_letters = "EFqQ"
    _too_many_letters = re.compile(r"([GMLEQq])\1{4,}|y{7,}")

    @staticmethod
    def _unquoted_parts(pattern: str) -> List[str]:
        """Return the parts of a pattern that are outside single-quoted sections."""
        return pattern.split("'")[0::2]

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check literal datetime patterns for letters Spark 3.0 rejects or reinterprets."""
        name_and_bracketed = _function_name_and_bracketed(context)
        if name_and_bracketed is None:
            return None
        function_name, bracketed = name_and_bracketed
        is_formatting = function_name in self.formatting_functions
        is_parsing = function_name in self.parsing_functions
        if not (is_formatting or is_parsing):
            return None
        arguments = _expression_arguments(bracketed)
        if len(arguments) < 2:
            return None
        literal = _single_quoted_literal(arguments[1])
        if literal is None:
            return None
        pattern = _literal_value(literal)
        unquoted = "".join(self._unquoted_parts(pattern))

        problems = []
        week_based = sorted(set(unquoted) & set(self.week_based_letters))
        if week_based:
            problems.append(
                f"week-based letter(s) {', '.join(repr(c) for c in week_based)} are rejected "
                "(All week-based patterns are unsupported since Spark 3.0); use "
                "weekofyear()/dayofweek()/EXTRACT instead"
            )
        illegal = sorted(set(unquoted) & set(self.illegal_letters))
        if illegal:
            problems.append(
                f"letter(s) {', '.join(repr(c) for c in illegal)} are illegal pattern "
                "characters in Spark 3.0+"
            )
        if is_parsing:
            formatting_only = sorted(set(unquoted) & set(self.formatting_only_letters))
            if formatting_only:
                problems.append(
                    f"letter(s) {', '.join(repr(c) for c in formatting_only)} are only "
                    "valid for formatting (date_format/from_unixtime), not for parsing; "
                    "remove the field from the pattern"
                )
        too_many = []
        for part in self._unquoted_parts(pattern):
            too_many.extend(match.group(0) for match in self._too_many_letters.finditer(part))
        if too_many:
            problems.append(
                f"too many pattern letters in {', '.join(repr(r) for r in too_many)} "
                "(narrow text style was full-form in 2.4); use at most 4"
            )
        if is_formatting and "F" in unquoted:
            problems.append(
                "'F' now means aligned day-of-week-in-month, not day-of-week-in-month "
                "(week of month) as in 2.4; use EXTRACT for the old value"
            )
        if not problems:
            return None
        return LintResult(
            anchor=literal,
            description=f"Datetime pattern {pattern!r} in {function_name.lower()}() breaks "
            "under the Spark 3.0 DateTimeFormatter: " + "; ".join(problems) + ". To keep "
            "the 2.4 behaviour set spark.sql.legacy.timeParserPolicy=LEGACY. See "
            "https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html",
        )


class Rule_COUNTQUALIFIEDSTAR_L010(BaseRule):
    """Spark 3.2 rejects count(tblName.*).

    Since Spark 3.2 ``count(t.*)`` raises ``AnalysisException: count(tblName.*) is not
    allowed``. ``count(*)`` counts all rows while ``count(t.*)`` in 3.1 counted only
    rows where every column of ``t`` is non-null, so they are not interchangeable and
    no automatic rewrite is done. ``count(t1.*, t2.*)`` with several arguments is still
    allowed.

    **Flagged**

    .. code-block:: sql

        SELECT count(t.*), count(DISTINCT t.*) FROM t

    **Not flagged**

    .. code-block:: sql

        SELECT count(*), count(t.c1), count(t1.*, t2.*) FROM t1 JOIN t2
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Warn when count() has exactly one argument and it is a qualified star."""
        name_and_bracketed = _function_name_and_bracketed(context)
        if name_and_bracketed is None:
            return None
        function_name, bracketed = name_and_bracketed
        if function_name != "COUNT":
            return None
        # Split the call's arguments on commas, ignoring the brackets and DISTINCT/ALL.
        # count(t.*) parses as one expression [naked_identifier, dot, star]; count(db.t.*)
        # as expression [column_reference 'db.t.'] followed by a sibling star, so work on the
        # flattened raw tokens of each argument rather than on the expression segments.
        groups: List[list] = [[]]
        for child in bracketed.segments:
            if not child.is_code or child.is_type("start_bracket", "end_bracket", "keyword"):
                continue
            if child.is_type("comma"):
                groups.append([])
                continue
            groups[-1].extend(raw for raw in child.raw_segments if raw.is_code)
        if len(groups) != 1:
            # count(t1.*, t2.*) with several arguments remains allowed.
            return None
        tokens = groups[0]
        # count(*) is a bare star; a qualified star ends with [..., dot, star].
        if len(tokens) < 3 or not tokens[-1].is_type("star") or not tokens[-2].is_type("dot"):
            return None
        qualifier = "".join(token.raw for token in tokens[:-2])
        return LintResult(
            anchor=context.segment,
            description=f"count({qualifier}.*) is rejected since Spark 3.2 (AnalysisException: "
            "count(tblName.*) is not allowed). count(*) counts all rows while "
            f"count({qualifier}.*) in 3.1 counted only rows where every column of "
            f"{qualifier} is non-null, so they are not interchangeable. Choose: count(*) "
            f"if you want a row count; count({qualifier}.c1, {qualifier}.c2, ...) listing "
            "the columns to keep the 3.1 semantics; or set "
            "spark.sql.legacy.allowStarWithSingleTableIdentifierInCount=true. Note: "
            "count(t1.*, t2.*) with multiple arguments remains allowed. See "
            "https://spark.apache.org/docs/latest/sql-migration-guide.html"
            "#upgrading-from-spark-sql-31-to-32",
        )


class Rule_ENCODECHARSET_L011(BaseRule):
    """Spark 4.0 encode()/decode() only accept canonical charset names.

    Since Spark 4.0 ``encode``/``decode`` only accept US-ASCII, ISO-8859-1, UTF-8,
    UTF-16BE, UTF-16LE, UTF-16 and UTF-32, matched case-insensitively against the
    canonical name. JDK aliases that worked in 3.5 (``utf8``, ``latin1``, ``ascii``,
    ``utf16`` ...) now fail with INVALID_PARAMETER_VALUE.CHARSET. Aliases of a
    supported charset are rewritten to the canonical name (the same Charset instance
    on every version); any other charset literal is only warned about.

    **Spark 3.5**

    .. code-block:: sql

        SELECT encode(a, 'utf8'), decode(b, 'latin1') FROM t

    **Spark 4.0**

    .. code-block:: sql

        SELECT encode(a, 'UTF-8'), decode(b, 'ISO-8859-1') FROM t
    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler({"function"})
    is_fix_compatible = True

    supported_charsets = {
        "us-ascii",
        "iso-8859-1",
        "utf-8",
        "utf-16be",
        "utf-16le",
        "utf-16",
        "utf-32",
    }
    # Lower-cased JDK Charset.aliases() -> canonical name (verified against a live JDK).
    _jdk_aliases = {
        "UTF-8": ["utf8", "unicode-1-1-utf-8"],
        "ISO-8859-1": [
            "latin1",
            "l1",
            "iso8859-1",
            "iso8859_1",
            "iso_8859-1",
            "iso_8859_1",
            "iso_8859-1:1987",
            "iso-ir-100",
            "8859_1",
            "819",
            "cp819",
            "ibm819",
            "ibm-819",
            "csisolatin1",
        ],
        "US-ASCII": [
            "ascii",
            "ascii7",
            "us",
            "646",
            "iso646-us",
            "iso_646.irv:1991",
            "iso_646.irv:1983",
            "ansi_x3.4-1968",
            "ansi_x3.4-1986",
            "iso-ir-6",
            "cp367",
            "ibm367",
            "csascii",
        ],
        "UTF-16": ["utf16", "utf_16", "unicode", "unicodebig"],
        "UTF-16BE": ["utf_16be", "x-utf-16be", "iso-10646-ucs-2", "unicodebigunmarked"],
        "UTF-16LE": ["utf_16le", "x-utf-16le", "unicodelittleunmarked"],
        "UTF-32": ["utf32", "utf_32"],
    }
    alias_to_canonical = {
        alias: canonical for canonical, aliases in _jdk_aliases.items() for alias in aliases
    }

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check the charset literal of encode()/decode()."""
        name_and_bracketed = _function_name_and_bracketed(context)
        if name_and_bracketed is None:
            return None
        function_name, bracketed = name_and_bracketed
        if function_name not in ("ENCODE", "DECODE"):
            return None
        arguments = _expression_arguments(bracketed)
        if len(arguments) != 2:
            # Excludes the Oracle-style decode(expr, search, result, ..., default) overload.
            return None
        literal = _single_quoted_literal(arguments[1])
        if literal is None:
            return None
        charset = _literal_value(literal)
        charset_lower = charset.lower()
        if charset_lower in self.supported_charsets:
            return None
        canonical = self.alias_to_canonical.get(charset_lower)
        if canonical is not None:
            return LintResult(
                anchor=literal,
                description=f"Spark 4.0 encode()/decode() only accept canonical charset names; "
                f"'{charset}' is a JDK alias of '{canonical}' -- rewritten.",
                fixes=[LintFix.replace(literal, [literal.edit(f"'{canonical}'")])],
            )
        return LintResult(
            anchor=literal,
            description=f"charset '{charset}' is rejected by encode()/decode() in Spark 4.0 "
            "(only US-ASCII, ISO-8859-1, UTF-8, UTF-16BE, UTF-16LE, UTF-16, UTF-32 are "
            "supported). Set spark.sql.legacy.javaCharsets=true to keep accepting JDK "
            "charsets, or convert the data to a supported charset. Note also that "
            "unmappable characters now raise MALFORMED_CHARACTER_CODING instead of "
            "producing replacement characters; set spark.sql.legacy.codingErrorAction=true "
            "to restore the old behaviour. See "
            "https://spark.apache.org/docs/latest/sql-migration-guide.html"
            "#upgrading-from-spark-sql-35-to-40",
        )
