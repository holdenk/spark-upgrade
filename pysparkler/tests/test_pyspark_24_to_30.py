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

from tests.conftest import rewrite

from pysparkler.pyspark_24_to_30 import RequiredPandasVersionCommentWriter


def test_adds_required_pandas_version_comment_to_import_statements_without_alias():
    given_code = """
import pandas
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher
import pyspark
"""
    assert modified_code == expected_code


def test_adds_required_pandas_version_comment_to_import_statements_with_alias():
    given_code = """
import pandas as pd
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas as pd  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher
import pyspark
"""
    assert modified_code == expected_code


def test_adds_required_pandas_version_comment_to_from_import_statements_without_alias():
    given_code = """
from pandas import DataFrame
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
from pandas import DataFrame  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher
import pyspark
"""
    assert modified_code == expected_code


def test_adds_required_pandas_version_comment_to_from_import_statements_with_alias():
    given_code = """
from pandas import DataFrame as df
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
from pandas import DataFrame as df  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher
import pyspark
"""
    assert modified_code == expected_code


def test_required_pandas_version_comment_idempotency():
    given_code = """
import pandas as pd # PY24-30-001: An existing comment added by this transformer
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas as pd # PY24-30-001: An existing comment added by this transformer
import pyspark
"""
    assert modified_code == expected_code


def test_does_not_overwrite_an_exising_user_comment():
    given_code = """
import pandas as pd # An existing comment
import pyspark
"""
    modified_code = rewrite(given_code, RequiredPandasVersionCommentWriter())
    expected_code = """
import pandas as pd # An existing comment  # PY24-30-001: PySpark 3.0 requires pandas version 0.23.2 or higher
import pyspark
"""
    assert modified_code == expected_code
