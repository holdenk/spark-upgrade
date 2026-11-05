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

from pysparkler.pyspark_31_to_32 import (
    FunctionsSnakeCaseRenamed,
    SqlMlMethodsRaiseTypeErrorCommentWriter,
)
from tests.conftest import rewrite


def test_adds_may_raise_type_error_when_catching_value_errors_on_sql_or_ml_from_imports():
    given_code = """
from pyspark.sql.column import Column, _to_seq, _to_list, _to_java_column
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.streaming import DataStreamWriter


def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    modified_code = rewrite(given_code, SqlMlMethodsRaiseTypeErrorCommentWriter())
    expected_code = """
from pyspark.sql.column import Column, _to_seq, _to_list, _to_java_column
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.streaming import DataStreamWriter


def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError:  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.  # noqa: E501
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    assert modified_code == expected_code


def test_adds_may_raise_type_error_with_alias_when_catching_value_errors_on_sql_or_ml_from_imports():
    given_code = """
from pyspark.ml.streaming import DataStreamWriter

def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError as ex:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    modified_code = rewrite(given_code, SqlMlMethodsRaiseTypeErrorCommentWriter())
    expected_code = """
from pyspark.ml.streaming import DataStreamWriter

def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError as ex:  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.  # noqa: E501
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    assert modified_code == expected_code


def test_adds_may_raise_type_error_with_alias_when_catching_value_errors_on_sql_or_ml_import():
    given_code = """
import pyspark.sql.functions as f

def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError as ex:
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    modified_code = rewrite(given_code, SqlMlMethodsRaiseTypeErrorCommentWriter())
    expected_code = """
import pyspark.sql.functions as f

def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:

    if isinstance(truncate, bool) and truncate:
        print(self._jdf.showString(n, 20, vertical))
    else:
        try:
            int_truncate = int(truncate)
        except ValueError as ex:  # PY31-32-001: As of PySpark 3.2, the methods from sql, ml, spark_on_pandas modules raise the TypeError instead of ValueError when are applied to a param of inappropriate type.  # noqa: E501
            raise TypeError(
                "Parameter 'truncate={}' should be either bool or int.".format(truncate)
            )

        print(self._jdf.showString(n, int_truncate, vertical))
"""
    assert modified_code == expected_code


HINT_PY31_32_002 = (
    "# PY31-32-002: Renamed to the snake_case function added in PySpark 3.2 (the camelCase alias emits "
    "FutureWarning; sum_distinct/bitwise_not/shiftleft/shiftright/shiftrightunsigned require PySpark >= 3.2)."
    "  # noqa: E501"
)


def test_renames_camel_case_functions_on_module_alias():
    given_code = """
import pyspark.sql.functions as F
from pyspark.sql import functions
from pyspark.sql import functions as sf
a = F.sumDistinct(df.a)
b = functions.shiftLeft(df.a, 2)
c = sf.bitwiseNOT(df.a)
d = pyspark.sql.functions.shiftRightUnsigned(df.a, 1)
e = F.shiftRight(df.a, 1)
f = F.toDegrees(df.a)
g = F.toRadians(df.a)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    expected_code = f"""
import pyspark.sql.functions as F
from pyspark.sql import functions
from pyspark.sql import functions as sf
a = F.sum_distinct(df.a)  {HINT_PY31_32_002}
b = functions.shiftleft(df.a, 2)  {HINT_PY31_32_002}
c = sf.bitwise_not(df.a)  {HINT_PY31_32_002}
d = pyspark.sql.functions.shiftrightunsigned(df.a, 1)  {HINT_PY31_32_002}
e = F.shiftright(df.a, 1)  {HINT_PY31_32_002}
f = F.degrees(df.a)  {HINT_PY31_32_002}
g = F.radians(df.a)  {HINT_PY31_32_002}
"""
    assert modified_code == expected_code


def test_renames_camel_case_functions_in_from_import_and_bare_uses():
    given_code = """
from pyspark.sql.functions import col, sumDistinct, approxCountDistinct as acd
a = sumDistinct(df.a)
b = acd(df.a, rsd=0.05)
c = approxCountDistinct(df.a)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    expected_code = f"""
from pyspark.sql.functions import col, sum_distinct, approxCountDistinct as acd  {HINT_PY31_32_002}
a = sum_distinct(df.a)  {HINT_PY31_32_002}
b = acd(df.a, rsd=0.05)
c = approxCountDistinct(df.a)
"""
    assert modified_code == expected_code


def test_renames_camel_case_functions_preserving_keyword_arguments():
    given_code = """
import pyspark.sql.functions as F
a = F.approxCountDistinct(col=df.a, rsd=0.1)
b = F.shiftLeft(df.a, numBits=2)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert (
        f"a = F.approx_count_distinct(col=df.a, rsd=0.1)  {HINT_PY31_32_002}"
        in modified_code
    )
    assert f"b = F.shiftleft(df.a, numBits=2)  {HINT_PY31_32_002}" in modified_code


def test_does_not_rename_camel_case_functions_under_star_import():
    given_code = """
from pyspark.sql.functions import *
a = sumDistinct(df.a)
b = shiftLeft(df.a, 2)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert modified_code == given_code


def test_does_not_rename_camel_case_methods_on_unrelated_receivers():
    given_code = """
import pyspark.sql.functions as F
q = bits.shiftLeft(2)
r = stats.sumDistinct()
s = shiftLeft(x, 2)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert modified_code == given_code


def test_does_not_rename_camel_case_functions_redefined_by_the_module():
    given_code = """
from pyspark.sql.functions import shiftLeft, sumDistinct

def shiftLeft(c, n):
    return c * (2 ** n)

sumDistinct = my_sum_distinct
a = shiftLeft(df.a, 2)
b = sumDistinct(df.a)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert modified_code == given_code


def test_camel_case_function_rename_is_idempotent():
    given_code = """
import pyspark.sql.functions as F
from pyspark.sql.functions import sumDistinct
a = F.shiftLeft(df.a, 2)
b = sumDistinct(df.a)
"""
    modified_once = rewrite(given_code, FunctionsSnakeCaseRenamed())
    modified_twice = rewrite(modified_once, FunctionsSnakeCaseRenamed())
    assert modified_once == modified_twice
    assert modified_once.count("# PY31-32-002:") == 3


def test_renames_every_bare_reference_to_an_unaliased_import_not_only_calls():
    given_code = """
from pyspark.sql.functions import shiftLeft, sumDistinct
from functools import partial
fn = shiftLeft
agg = {"x": sumDistinct}
p = partial(shiftLeft, numBits=2)
df.select(*map(sumDistinct, cols))
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    expected_code = f"""
from pyspark.sql.functions import shiftleft, sum_distinct  {HINT_PY31_32_002}
from functools import partial
fn = shiftleft  {HINT_PY31_32_002}
agg = {{"x": sum_distinct}}  {HINT_PY31_32_002}
p = partial(shiftleft, numBits=2)  {HINT_PY31_32_002}
df.select(*map(sum_distinct, cols))  {HINT_PY31_32_002}
"""
    assert modified_code == expected_code
    compile(modified_code, "<test>", "exec")


def test_does_not_rename_imports_with_an_explicit_alias_or_compat_shims():
    given_code = """
from pyspark.sql.functions import shiftLeft as shiftLeft
a = shiftLeft(c, 1)
try:
    from pyspark.sql.functions import shiftleft
except ImportError:
    from pyspark.sql.functions import shiftLeft as shiftleft
b = shiftleft(c, 1)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert modified_code == given_code


def test_does_not_rename_keyword_arguments_or_attributes_sharing_a_renamed_name():
    given_code = """
from pyspark.sql.functions import shiftLeft
a = other(shiftLeft=1)
b = bits.shiftLeft(2)
c = shiftLeft(c, 1)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert "a = other(shiftLeft=1)\n" in modified_code
    assert "b = bits.shiftLeft(2)\n" in modified_code
    assert f"c = shiftleft(c, 1)  {HINT_PY31_32_002}" in modified_code


def test_rename_hint_lands_on_the_compound_statement_header():
    given_code = """
import pyspark.sql.functions as F

def f(c):
    if F.shiftLeft(c, 1):
        x = 1
    for r in df.select(F.sumDistinct(c)).collect():
        pass
    return F.shiftRight(c, 1)

@decorator(F.toDegrees(c))
def g(c=F.toRadians(c)):
    '''doc'''

class K(Base, metaclass=F.bitwiseNOT):
    y = 2

if F.shiftLeft(c, 1): z = 1
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    expected_code = f"""
import pyspark.sql.functions as F

def f(c):
    if F.shiftleft(c, 1):  {HINT_PY31_32_002}
        x = 1
    for r in df.select(F.sum_distinct(c)).collect():  {HINT_PY31_32_002}
        pass
    return F.shiftright(c, 1)  {HINT_PY31_32_002}

@decorator(F.degrees(c))
def g(c=F.radians(c)):  {HINT_PY31_32_002}
    '''doc'''

class K(Base, metaclass=F.bitwise_not):  {HINT_PY31_32_002}
    y = 2

if F.shiftleft(c, 1): z = 1  {HINT_PY31_32_002}
"""
    assert modified_code == expected_code


def test_does_not_rename_names_rebound_by_for_with_comprehension_or_walrus_targets():
    given_code = """
from pyspark.sql.functions import shiftLeft, shiftRight, sumDistinct, bitwiseNOT
for shiftLeft in fns:
    a = shiftLeft(df.a, 2)
with ctx() as shiftRight:
    b = shiftRight(df.a, 2)
c = [sumDistinct for sumDistinct in fns]
if (bitwiseNOT := pick()):
    d = bitwiseNOT(df.a)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert modified_code == given_code


def test_does_not_rename_on_a_module_alias_that_is_rebound():
    given_code = """
import pyspark.sql.functions as F
F = SomethingElse()
a = F.shiftLeft(c, 1)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert modified_code == given_code


def test_does_not_rename_through_a_parent_package_alias():
    given_code = """
import pyspark.sql as psql
from pyspark import sql
a = psql.functions.shiftLeft(c, 1)
b = sql.functions.shiftLeft(c, 1)
"""
    modified_code = rewrite(given_code, FunctionsSnakeCaseRenamed())
    assert modified_code == given_code
