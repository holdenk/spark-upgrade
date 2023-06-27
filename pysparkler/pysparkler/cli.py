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

import difflib
from collections.abc import Callable
from functools import wraps
from importlib import metadata
from typing import Any

import click
import yaml
from click import Context
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from pysparkler.api import PySparkler
from pysparkler.sql_21_to_33 import SqlStatementUpgradeAndCommentWriter

stdout = Console()
stderr = Console(stderr=True)


def catch_exception() -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            try:
                return func(*args, **kwargs)
            except Exception as e:  # pylint: disable=broad-except
                ctx: Context = click.get_current_context(silent=True)
                if ctx.obj["verbose"]:
                    stderr.print_exception()
                else:
                    stderr.print(e)
                ctx.exit(1)

        return wrapper

    return decorator


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Verbose mode")
@click.option(
    "-c",
    "--config-yaml",
    type=click.Path(exists=True, readable=True),
    help="Config YAML to customize PySparkler behavior. Check documentation for more details.",
)
@click.pass_context
def run(ctx: Context, verbose: bool, config_yaml: str) -> None:
    """Pysparkler CLI - A tool to upgrade PySpark scripts to newer versions"""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["config"] = {}

    if config_yaml:
        # Load configuration from YAML file
        with open(config_yaml, encoding="utf-8") as file:
            config = yaml.load(file, Loader=yaml.SafeLoader)

        # Set the configuration in PySparkler
        if "pysparkler" in config:
            ctx.obj["config"] = config["pysparkler"]


@run.command()
@click.option(
    "-i",
    "--input-file",
    required=True,
    type=click.Path(exists=True, readable=True),
    help="The input file to upgrade. This is re-written in place if no output file is provided and not in dry-run mode",
)
@click.option(
    "-o",
    "--output-file",
    type=click.Path(exists=False, writable=True),
    help="Output file to be written against the input. This is ignored in dry-run mode.",
)
@click.option(
    "-t",
    "--file-type",
    type=click.Choice(["py", "ipynb"]),
    help="To override the input file type, by default PySparkler infers this from input file extension.",
)
@click.option(
    "-k",
    "--output-kernel",
    type=click.STRING,
    help="The kernel name in the output Jupyter Notebook. This option is ignored if the file type is not `ipynb`",
)
@click.option(
    "-d",
    "--dry-run",
    is_flag=True,
    help="Dry run mode, just shows a unified diff across input and output.",
)
@click.pass_context
@catch_exception()
def upgrade(
    ctx: Context,
    input_file: str,
    output_file: str | None,
    dry_run: bool,
    file_type: str | None,
    output_kernel: str | None,
) -> None:
    """Upgrades a PySpark script or Jupyter Notebook to the latest version, and where not context, provides comments as
    code hints for manual changes.
    """

    print_command_params(ctx)
    if "dry_run" not in ctx.obj["config"]:
        ctx.obj["config"]["dry_run"] = dry_run

    pysparkler = PySparkler(**ctx.obj["config"])

    file_type = file_type or input_file.split(".")[-1]
    if file_type == "ipynb":
        output_file_content = pysparkler.upgrade_notebook(
            input_file, output_file, output_kernel
        )
    else:
        output_file_content = pysparkler.upgrade_script(input_file, output_file)

    with open(input_file, encoding="utf-8") as f:
        input_file_content = f.read()

    if input_file_content == output_file_content:
        stdout.print(
            f"No upgrades detected in {output_file or input_file}", style="green"
        )
        return None

    lexer = (
        "python" if file_type == "py" else "json" if file_type == "ipynb" else file_type
    )

    if ctx.obj["verbose"]:
        stdout.rule("Input")
        stdout.print(Syntax(input_file_content, lexer, line_numbers=True))
        stdout.rule("Output")
        stdout.print(Syntax(output_file_content, lexer, line_numbers=True))

    if dry_run:
        stdout.rule("Unified Diff")
        diff = difflib.unified_diff(
            input_file_content.splitlines(), output_file_content.splitlines()
        )
        for line in diff:
            stdout.print(Syntax(line, lexer))
        stdout.rule("End of Diff")
    else:
        stdout.print(f"Output written to {output_file or input_file}", style="green")


@run.command()
@click.pass_context
@catch_exception()
def version(ctx: Context) -> None:
    """Prints version and other metadata about package PySparkler"""
    stdout.print(f"PySparkler Version: {metadata.version('pysparkler')}")
    if ctx.obj["verbose"]:
        table = Table(title="PySparkler Metadata")
        table.add_column("Metadata")
        table.add_column("Value")

        for key, value in metadata.metadata("pysparkler").items():  # type: ignore
            table.add_row(key, str(value))

        stdout.print(table)


@run.command()
@click.pass_context
@catch_exception()
def upgrade_sql(ctx: Context) -> None:
    """Upgrades a non-templated Spark SQL statement read from stdin to be compatible with the latest Spark version.

    Examples: \n
        echo "SELECT * FROM table" | pysparkler upgrade-sql \n
        cat /path/to/input.sql | pysparkler upgrade-sql
    """

    # Read SQL from stdin
    input_sql = click.get_text_stream("stdin").read()
    if ctx.obj["verbose"]:
        stdout.rule("Input SQL")
        stdout.print(Syntax(input_sql, "sql", line_numbers=True))

    # Ensure SQL is parsable and upcast the SQL to be compatible with the latest Spark version
    SqlStatementUpgradeAndCommentWriter.do_parse(input_sql)
    output_sql = SqlStatementUpgradeAndCommentWriter.do_fix(input_sql)

    if input_sql == output_sql:
        stdout.print("No upgrades detected in Input SQL", style="green")
    else:
        # Output the upcasted SQL to stdout
        stdout.rule("Output SQL")
        stdout.print(Syntax(output_sql, "sql", line_numbers=True))
        stdout.rule("Unified Diff")
        diff = difflib.unified_diff(input_sql.splitlines(), output_sql.splitlines())
        for line in diff:
            stdout.print(Syntax(line, "sql"))
        stdout.rule("End of Diff")


def print_command_params(ctx):
    """Prints the command params"""
    if ctx.obj["verbose"]:
        table = Table(title="PySparkler CLI Parameters")
        table.add_column("Param")
        table.add_column("Value")

        for key, value in ctx.obj.items():
            table.add_row(key, str(value))

        for key, value in ctx.params.items():
            table.add_row(key, str(value))

        stdout.print(table)


if __name__ == "__main__":
    run()
