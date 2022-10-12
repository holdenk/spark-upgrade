"""An example of a custom rule implemented through the plugin system."""

from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.rules import (
    BaseRule,
    LintResult,
    LintFix,
    RuleContext,
)
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.core.rules.doc_decorators import (
    document_configuration,
    document_fix_compatible,
    document_groups,
)
from sqlfluff.core.parser.segments.raw import CodeSegment
from sqlfluff.utils.functional import sp, FunctionalContext
from typing import List, Optional
import os.path
from sqlfluff.core.config import ConfigLoader


@hookimpl
def get_rules() -> List[BaseRule]:
    """Get plugin rules."""
    return [Rule_Example_L001, Rule_SPARKSQLCAST_L001, Rule_RESERVEDROPERTIES_L002]


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


@document_groups
@document_fix_compatible
@document_configuration
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

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check integer casts."""
        functional_context = FunctionalContext(context)
        children = functional_context.segment.children()
        function_name_id_seg = children.first(sp.is_type("function_name")).children(
        ).first(sp.is_type("function_name_identifier"))[0]
        raw_function_name = function_name_id_seg.raw.upper().strip()
        function_name = raw_function_name.upper().strip()
        bracketed_segments = children.first(sp.is_type("bracketed"))
        bracketed = bracketed_segments[0]

        # Is this a cast function call
        if function_name == "CAST":
            print("Found cast function!")
            data_type_info = bracketed.get_child(
                "data_type").raw.upper().strip()
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
                            [
                                function_name_id_seg.edit(
                                    f"int({expr.raw})"
                                )
                            ],
                        ),
                        LintFix.delete(
                            bracketed,
                        ),
                    ],
                )

        return None


@document_groups
@document_fix_compatible
@document_configuration
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

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check for reserved properties being configured."""
        functional_context = FunctionalContext(context)
        property_name_segment = context.segment
        property_name = property_name_segment.raw.lower().strip().lstrip('\"').rstrip('\"')
        print(f"Called with context {context} with \"{property_name}\"")
        if (property_name not in self.reserved):
            return None
        else:
            print("Yee haw!")
            # Now we want to get the segments that are "bad" (e.g. we want to delete) and that is
            # everything from this segment up until either a comma segment or a endbracket segment.
            parent_segment = context.parent_stack[-1]
            print(f"{dir(functional_context)}")
            siblings_post = functional_context.siblings_post
            segments_to_remove = [property_name_segment]
            property_value = None
            for segment in siblings_post:
                # We want to keep the end bracket so check for it before adding to the list
                if segment.is_type("end_bracket"):
                    break
                segments_to_remove.append(segment)
                if segment.is_type("quoted_literal"):
                    property_value = segment.raw.strip().lstrip('\"').rstrip('\"')
                print(f"{segment} - {segment.get_type()}")
                # We want to drop the comma so we do the check _after_ the ops
                if segment.is_type("comma"):
                    break
            deletes = map(lambda t: LintFix.delete(t), segments_to_remove)
            new_statement = None
            if property_name == "provider":
                new_segment = CodeSegment(raw=f" USING {property_value}")
                print(functional_context.parent_stack)
                create_table_segment = functional_context.parent_stack[-2]
                # We want to insert after the first bracketed segment containing column_definition
                # but if there are no column definitions we instead insert after the table identifier.
                first_bracketed_segment = create_table_segment.get_child(
                    "bracketed")
                print(dir(first_bracketed_segment))
                if "column_definition" in first_bracketed_segment.direct_descendant_type_set:
                    new_statement = LintFix.create_after(
                        first_bracketed_segment,
                        [new_segment],
                    )
                else:
                    new_statement = LintFix.create_after(
                        create_table_segment.get_child("table_reference"),
                        [new_segment],
                    )
#            elif property_name == "location":
#                new_statement = LintFix.create_after(
#                    siblings_post[-1],
#                    [CodeSegment(raw=f" LOCATION(\"{property_value}\")")],
#                    )
            else:
                # For "owner" property we don't have an easy work around so instead just raise a lint error.
                return LintResult(
                    anchor=context.segment,
                    description=f"Reserved table/db property {property_name} found see " +
                    "https://spark.apache.org/docs/3.0.0/sql-migration-guide.html for migration advice.",
                    fixes=None)
            fixes = list(deletes) + [new_statement]
            return LintResult(
                anchor=context.segment,
                description="Reserved table property found.",
                fixes=fixes)
            # TODO - Make a rewrite rule.


# These two decorators allow plugins
# to be displayed in the sqlfluff docs
@document_groups
@document_fix_compatible
@document_configuration
class Rule_Example_L001(BaseRule):
    """ORDER BY on these columns is forbidden!

    **Anti-pattern**

    Using ``ORDER BY`` one some forbidden columns.

    .. code-block:: sql

        SELECT *
        FROM foo
        ORDER BY
            bar,
            baz

    **Best practice**

    Do not order by these columns.

    .. code-block:: sql

        SELECT *
        FROM foo
        ORDER BY bar
    """

    groups = ("all",)
    config_keywords = ["forbidden_columns"]
    crawl_behaviour = SegmentSeekerCrawler({"column_reference"})

    def __init__(self, *args, **kwargs):
        """Overwrite __init__ to set config."""
        super().__init__(*args, **kwargs)
        self.forbidden_columns = [
            col.strip() for col in self.forbidden_columns.split(",")
        ]

    def _eval(self, context: RuleContext):
        """We should not use ORDER BY."""
        if context.segment.is_type("orderby_clause"):
            for seg in context.segment.segments:
                col_name = seg.raw.lower()
                if col_name in self.forbidden_columns:
                    return LintResult(
                        anchor=seg,
                        description=f"Column `{col_name}` not allowed in ORDER BY.",
                    )
