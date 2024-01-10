"""Custom Spark SQL upgrade rules."""

import os.path
from typing import List


from sqlfluff.core.config import ConfigLoader
from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.rules import BaseRule


@hookimpl
def get_rules() -> List[BaseRule]:
    """Get plugin rules."""
    from .rules import (
        Rule_SPARKSQLCAST_L001,
        Rule_RESERVEDROPERTIES_L002,
        Rule_NOCHARS_L003,
        Rule_FORMATSTRONEINDEX_L004,
        Rule_SPARKSQL_L004,
        Rule_SPARKSQL_L005,
    )

    return [
        Rule_SPARKSQLCAST_L001,
        Rule_RESERVEDROPERTIES_L002,
        Rule_NOCHARS_L003,
        Rule_FORMATSTRONEINDEX_L004,
        Rule_SPARKSQL_L004,
        Rule_SPARKSQL_L005,
    ]


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
