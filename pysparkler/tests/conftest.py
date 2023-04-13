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

import os

import libcst as cst


def rewrite(given_code: str, cst_transformer: cst.CSTTransformer):
    given_tree = cst.parse_module(given_code)
    modified_tree = given_tree.visit(cst_transformer)
    modified_code = modified_tree.code
    return modified_code


def absolute_path(relative_path: str):
    cwd = os.getcwd()
    # Tokenize on the path separator
    relative_path_tokens = relative_path.split(os.path.sep)
    cwd_tokens = cwd.split(os.path.sep)

    # Check if last token of cwd is the same as the first token of the relative path
    if cwd_tokens[-1] == relative_path_tokens[0]:
        # Remove the first token of the relative path
        relative_path_tokens.pop(0)
        # Join the remaining tokens
        relative_path = os.path.sep.join(relative_path_tokens)
        # Return the relative path

    # Join with the current directory to make the absolute path
    return os.path.join(cwd, relative_path)
