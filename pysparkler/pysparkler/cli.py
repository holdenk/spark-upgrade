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

from pysparkler.api import PySparkler


@click.group()
@click.option("-v", "--verbose", type=click.BOOL)
@click.option("-d", "--dry-run", type=click.BOOL)
@click.pass_context
def run(ctx: Context, verbose: bool, dry_run: bool) -> None:
    """Pysparkler CLI - A tool to upgrade PySpark scripts to newer versions"""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["dry_run"] = dry_run


@run.command()
@click.option("-o", "--output-file", type=click.Path(exists=False, writable=True))
@click.argument("input_file", type=click.Path(exists=True, readable=True))
def upgrade(input_file: str, output_file: str | None) -> None:
    """Upgrade the PySparkler file to the latest version and provides comments as hints for manual changes"""

    click.echo(f"Input File: {input_file}")
    if output_file:
        click.echo(f"Output File: {output_file}")

    pysparkler = PySparkler()
    output_file_content = pysparkler.upgrade(input_file)

    with open(input_file, encoding="utf-8") as f:
        input_file_content = f.read()

    click.echo("Unified Diff:")
    click.echo(
        "".join(
            difflib.unified_diff(
                input_file_content.splitlines(), output_file_content.splitlines()
            )
        )
    )
