import re,sys;

original_build = sys.stdin.read()

build_with_plugin = original_build

if "scalafix" not in build_with_plugin:
    build_with_plugin = re.sub(
        r"plugins\s*{",
        "plugins {\n    id 'io.github.cosmicsilence.scalafix' version '0.1.14'\n",
        build_with_plugin
    )

build_with_plugin_and_rules = re.sub(
    r"dependencies\s*{",
    "dependencies {\n    scalafix group: 'com.holdenkarau', name: 'spark-scalafix-rules-2.4.8_2.12', version: '0.1.9'\n",
    build_with_plugin)

print(build_with_plugin_and_rules)
