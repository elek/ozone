#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a simple checkout script specific to the
set -ex

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cat "$GITHUB_EVENT_PATH"

#BODY=$(jq -r .comment.body "$GITHUB_EVENT_PATH")

env

echo $GITHUB_REPOSITORY
echo $GITHUB_WORKSPACE
echo $GITHUB_SHA

rm -rf "$GITHUB_WORKSPACE"

git clone "https://github.com/$GITHUB_REPOSITORY" "$GITHUB_WORKSPACE"

git checkout "$GITHUB_SHA"

cd "$GITHUB_WORKSPACE"

git log -1
