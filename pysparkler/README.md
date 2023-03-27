# PySparkler

## About

PySparkler is a tool that upgrades your PySpark scripts to Spark 3.3. It is a command line tool that takes a PySpark
script as input and outputs a Spark 3.3 compatible script. It is written in Python and uses the
[LibCST](https://github.com/Instagram/LibCST) module to parse the input script and generate the output script.

## Basic Usage

Install from PyPI:

```bash
pip install pysparkler
```

Provide the path to the script you want to upgrade:

```bash
pysparkler upgrade --input-file /path/to/script.py
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

LibCST is a Python library that provides a concrete syntax tree (CST) for Python code. CST preserves even the whitespaces
of the source code which is very important since we only want to modify the code and not the formatting.

### How does it work?

Using the codemod module of LibCST can simplify the process of writing a PySpark migration script, as it allows us to
write small, reusable transformers and chain them together to perform a sequence of transformations.

### Why Transformer Codemod? Why not Visitor?

The main advantage of using a Transformer is that it allows for more fine-grained control over the transformation
process. Transformer classes can be defined to apply specific transformations to specific parts of the codebase, and
multiple Transformer classes can be combined to form a chain of transformations. This can be useful when dealing with
complex codebases where different parts of the code require different transformations.

More on this can be found [here](https://libcst.readthedocs.io/en/latest/tutorial.html#Build-Visitor-or-Transformer).
