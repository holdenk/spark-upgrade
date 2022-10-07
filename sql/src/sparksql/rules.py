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
from sqlfluff.utils.functional import sp, FunctionalContext
from typing import List, Optional
import os.path
from sqlfluff.core.config import ConfigLoader


@hookimpl
def get_rules() -> List[BaseRule]:
    """Get plugin rules."""
    return [Rule_Example_L001, Rule_SPARKSQLCAST_L001]


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
