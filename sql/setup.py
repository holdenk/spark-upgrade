"""Setup file for example plugin."""
from setuptools import find_packages, setup

# Change these names in your plugin, e.g. company name or plugin purpose.
PLUGIN_LOGICAL_NAME = "sparksql"
PLUGIN_ROOT_MODULE = "sparksql"

setup(
    name="sqlfluff-plugin-{plugin_logical_name}".format(
        plugin_logical_name=PLUGIN_LOGICAL_NAME
    ),
    version='',
    author='Holden Karau',
    author_email='holden@pigscanfly.ca',
    url='https://github.com/holdenk/spark-upgrade',
    description='SQLFluff rules to help migrate your Spark SQL from 2.X to 3.X'
    test_requires=[
        'nose',
        'coverage',
        'unittest2'
    ],
    license='../LICENSE'
    include_package_data=True,
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires="sqlfluff>=1.0.0",
    entry_points={
        "sqlfluff": [
            "{plugin_logical_name} = {plugin_root_module}.rules".format(
                plugin_logical_name=PLUGIN_LOGICAL_NAME,
                plugin_root_module=PLUGIN_ROOT_MODULE,
            )
        ]
    },
)
