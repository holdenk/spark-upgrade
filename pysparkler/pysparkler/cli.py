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

import click
from click import Context
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from pysparkler.api import PySparkler

stdout = Console()
stderr = Console(stderr=True, style="bold red")


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Verbose mode")
@click.pass_context
def run(ctx: Context, verbose: bool) -> None:
    """Pysparkler CLI - A tool to upgrade PySpark scripts to newer versions"""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


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
    "-d",
    "--dry-run",
    is_flag=True,
    help="Dry run mode, just shows a unified diff across input and output.",
)
@click.pass_context
def upgrade(
    ctx: Context,
    input_file: str,
    output_file: str | None,
    dry_run: bool,
) -> None:
    """Upgrade the PySparkler file to the latest version and provides comments as hints for manual changes"""

    print_command_params(ctx)
    pysparkler = PySparkler()
    output_file_content = pysparkler.upgrade(
        input_file, output_file if not dry_run else None
    )

    if ctx.obj["verbose"] or dry_run:
        with open(input_file, encoding="utf-8") as f:
            input_file_content = f.read()

    if ctx.obj["verbose"]:
        stdout.rule("Input")
        stdout.print(Syntax(input_file_content, "python"))
        stdout.rule("Output")
        stdout.print(Syntax(output_file_content, "python"))

    if dry_run:
        stdout.rule("Unified Diff")
        diff = difflib.unified_diff(
            input_file_content.splitlines(), output_file_content.splitlines()
        )
        for line in diff:
            stdout.print(Syntax(line, "python"))
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
