# PySparkler

[![PyPI version](https://badge.fury.io/py/pysparkler.svg)](https://badge.fury.io/py/pysparkler)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

PySparkler is a tool that upgrades your PySpark scripts to latest Spark version. It is a command line tool that takes a
PySpark script as input and outputs latest Spark version compatible script. It is written in Python and uses the
[LibCST](https://github.com/Instagram/LibCST) module to parse the input script and generate the output script.

## Installation

We recommend installing PySparkler from PyPI using [pipx](https://pypa.github.io/pipx) which allows us to install and
run Python Applications in Isolated Environments. To install pipx on your system, follow the instructions
[here](https://pypa.github.io/pipx/installation/#install-pipx). Once pipx is installed, you can install PySparkler using:

```bash
pipx install pysparkler
```

That's it! You are now ready to use PySparkler.

```bash
pysparkler --help
```

## Getting Started

Provide the path to the script you want to upgrade:

```bash
pysparkler upgrade --input-file /path/to/script.py
```

PySparkler parses the code and can perform either of the following actions:

- **Code Transformations** - These are modifications that are performed on the code to make it compatible with the
  latest Spark version. For example, if you are upgrading from Spark 2.4 to 3.0, PySparkler will alphabetically sort the
  keyword arguments in the `Row` constructor to preserve backwards compatible behavior. This action will also add a
  comment to the end of statement line being modified to indicate that the code was modified by PySparkler, explaining
  why.

- **Code Hints** - Python is a dynamically-typed language, so there are situations wherein PySparkler cannot, with
  100% accuracy, determine if the code is eligible for a transformation. In such situations, PySparkler adds code hints to
  guide the end-user to make an appropriate change if needed. Code hints are comments that are added to the end of a
  statement line to suggest changes that may be needed it to make it compatible with the latest Spark version.
  For example, if you are upgrading from Spark 2.4 to 3.0 and PySparkler detects `spark.sql.execution.arrow.enabled` is
  set to `True` in your code, it will add a code hint to the end of the line to suggest setting
  `spark.sql.execution.pandas.convertToArrowArraySafely` to `True` in case you want to raise errors in case of Integer
  overflow or Floating point truncation, instead of silent allows. As you can see the suggestion is pretty contextual and
  may not be applicable in all cases. In cases where not applicable, the end-user can choose to ignore the code hint.

**NOTE**: PySparkler tries to keep the code formatting intact as much as possible. However, it is possible that the
statement lines it takes actions on may fail the linting checks post changes. In such situations, the end-user will have
to fix the linting errors manually.

## PySpark Upgrades Supported

This tool follows the [Apache Spark Migration guide for PySpark](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html)
to upgrade your PySpark scripts. In the latest stable version it supports the following upgrades from the migration guide:

| Migration                                       | Supported | Details                                                                                                                                      |
|-------------------------------------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------|
| Upgrading from PySpark 3.3 to 3.4               | ❌         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-3-3-to-3-4)               |
| Upgrading from PySpark 3.2 to 3.3               | ✅         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-3-2-to-3-3)               |
| Upgrading from PySpark 3.1 to 3.2               | ✅         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-3-1-to-3-2)               |
| Upgrading from PySpark 2.4 to 3.0               | ✅         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-2-4-to-3-0)               |
| Upgrading from PySpark 2.3 to 2.4               | ✅         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-2-3-to-2-4)               |
| Upgrading from PySpark 2.3.0 to 2.3.1 and above | ✅         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-2-3-0-to-2-3-1-and-above) |
| Upgrading from PySpark 2.2 to 2.3               | ✅         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-2-2-to-2-3)               |
| Upgrading from PySpark 2.1 to 2.2               | ✅         | NA                                                                                                                                           |
| Upgrading from PySpark 1.4 to 1.5               | ❌         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-1-4-to-1-5)               |
| Upgrading from PySpark 1.0-1.2 to 1.3           | ❌         | [Link](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html#upgrading-from-pyspark-1-0-1-2-to-1-3)           |

## Features Supported

The tool supports the following features:

| Feature                                       | Supported |
|-----------------------------------------------|-----------|
| Upgrade PySpark Python script                 | ✅         |
| Upgrade PySpark Jupyter Notebook              | ✅         |
| Upgrade SQL                                   | ✅         |
| Dry-run Mode                                  | ✅         |
| Verbose Mode                                  | ✅         |
| Customize code transformers using YAML config | ✅         |

### Upgrade PySpark Python script

The tool can upgrade a PySpark Python script. It takes the path to the script as input and upgrades it in place:

```bash
pysparkler upgrade --input-file /path/to/script.py
```

If you want to output the upgraded script to a different directory, you can use the `--output-file` flag:

```bash
pysparkler upgrade --input-file /path/to/script.py --output-file /path/to/output.py
```

### Upgrade PySpark Jupyter Notebook

The tool can upgrade a PySpark Jupyter Notebook to latest Spark version. It takes the path to the notebook as input and
upgrades it in place:

```bash
pysparkler upgrade --input-file /path/to/notebook.ipynb
```

Similar to upgrading python scripts, if you want to output the upgraded notebook to a different directory, you can use
the `--output-file` flag:

```bash
pysparkler upgrade --input-file /path/to/notebook.ipynb --output-file /path/to/output.ipynb
```

To change the output kernel name in the output Jupyter notebook, you can use the `--output-kernel` flag:

```bash
pysparkler upgrade --input-file /path/to/notebook.ipynb --output-kernel spark33-python3
```

### Upgrade SQL

PySparkler when encounters a SQL statement in the input script makes an attempt to upgrade them. However, it is not
always possible to upgrade certain formatted string SQL statements that have complex expressions within. In such
cases the tool does leave code hints to let users know that they need to upgrade the SQL themselves.

To facilitate this, it exposes a command `upgrade-sql` for users to perform this DIY. The steps for that include:

1. De-template the SQL.
1. Upgrade the de-templated SQL using `pysparkler upgrade-sql`. See below for details.
1. Re-template the upgraded SQL.
1. Replace the old SQL with the upgraded SQL in the input script.

In order to perform step #2 i.e. you can either echo the SQL statement and pipe it to the tool:

```bash
echo "SELECT * FROM table" | pysparkler upgrade-sql
```

or you can use the `cat` command to pipe the SQL statement to the tool:

```bash
cat /path/to/sql.sql | pysparkler upgrade-sql
```

### Dry-Run Mode

For both the above upgrade options, to run in dry mode, you can use the `--dry-run` flag. This will not write the
upgraded script but will print a unified diff of the input and output scripts for you to inspect the changes:

```bash
pysparkler upgrade --input-file /path/to/script.py --dry-run
```

### Verbose Mode

For both the above upgrade options, to run in verbose mode, you can use the `--verbose` flag. This will print tool's
input variables, the input file content, the output content, and a unified diff of the input and output content:

```bash
pysparkler --verbose upgrade --input-file /path/to/script.py
```

### Customize code transformers using YAML config

The tool uses a YAML config file to customize the code transformers. The config file can be passed using the
`--config-yaml` flag:

```bash
pysparkler --config-yaml /path/to/config.yaml upgrade --input-file /path/to/script.py
```

The config file is a YAML file with the following structure:

```yaml
pysparkler:
  dry_run: false # Whether to run in dry-run mode
  PY24-30-001: # The code transformer ID
    comment: A new comment # The overriden code hint comment to be used by the code transformer
  PY24-30-002:
    enabled: false # Disable the code transformer
```

## Contributing

For the development, Poetry is used for packing and dependency management. You can install this using:

```bash
pip install poetry
```

If you have an older version of pip and virtualenv you need to update these:

```bash
pip install --upgrade virtualenv pip
```

### Installation

To get started, you can run `make install`, which installs Poetry and all the dependencies of the PySparkler library.
This also installs the development dependencies.

```bash
make install
```

If you don't want to install the development dependencies, you need to install using `poetry install --only main`.

If you want to install the library on the host, you can simply run `pip3 install -e .`. If you wish to use a virtual
environment, you can run `poetry shell`. Poetry will open up a virtual environment with all the dependencies set.

### IDE Setup

To set up IDEA with Poetry:

- Open up the Python project in IntelliJ
- Make sure that you're on latest master (that includes Poetry)
- Go to File -> Project Structure (⌘;)
- Go to Platform Settings -> SDKs
- Click the + sign -> Add Python SDK
- Select Poetry Environment from the left hand side bar and hit OK
- It can take some time to download all the dependencies based on your internet
- Go to Project Settings -> Project
- Select the Poetry SDK from the SDK dropdown, and click OK

For IDEA ≤2021 you need to install the
[Poetry integration as a plugin](https://plugins.jetbrains.com/plugin/14307-poetry/).

Now you're set using Poetry, and all the tests will run in Poetry, and you'll have syntax highlighting in the
pyproject.toml to indicate stale dependencies.

### Linting

`pre-commit` is used for autoformatting and linting:

```bash
make lint
```

Pre-commit will automatically fix the violations such as import orders, formatting etc. Pylint errors you need to fix
yourself.

In contrast to the name suggest, it doesn't run the checks on the commit. If this is something that you like, you can
set this up by running `pre-commit install`.

You can bump the integrations to the latest version using `pre-commit autoupdate`. This will check if there is a newer
version of `{black,mypy,isort,...}` and update the yaml.

### Testing

For Python, `pytest` is used a testing framework in combination with `coverage` to enforce 90%+ code coverage.

```bash
make test
```

To pass additional arguments to pytest, you can use `PYTEST_ARGS`. For example, to run pytest in verbose mode:

```bash
make test PYTEST_ARGS="-v"
```

## Architecture

### Why LibCST?

LibCST is a Python library that provides a concrete syntax tree (CST) for Python code. CST preserves even the
whitespaces of the source code which is very important since we only want to modify the code and not the formatting.

### How does it work?

Using the codemod module of LibCST can simplify the process of writing a PySpark migration script, as it allows us to
write small, reusable transformers and chain them together to perform a sequence of transformations.

### Why Transformer Codemod? Why not Visitor?

The main advantage of using a Transformer is that it allows for more fine-grained control over the transformation
process. Transformer classes can be defined to apply specific transformations to specific parts of the codebase, and
multiple Transformer classes can be combined to form a chain of transformations. This can be useful when dealing with
complex codebases where different parts of the code require different transformations.

More on this can be found [here](https://libcst.readthedocs.io/en/latest/tutorial.html#Build-Visitor-or-Transformer).
