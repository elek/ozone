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



#
# Can be executed with bats (https://github.com/bats-core/bats-core)
# bats gc_opts.bats (FROM THE CURRENT DIRECTORY)
#


@test "Clone repository as a PR" {
  SCRIPT_DIR="$(cd "$(dirname "${BASHSOURCE[0]}")" >/dev/null 2>&1 && pwd)"
   GITHUB_REF=refs/pull/13/merge
   GITHUB_SHA=a843179cb83b3eae25bc29491228cbd0184c6ce4
   GITHUB_RUN_ID=172496980
   GITHUB_EVENT_PATH="$SCRIPT_DIR"/pr_event.json
   GITHUB_EVENT_NAME=pull_request
   GITHUB_REPOSITORY=elek/hadoop-ozone
   GITHUB_WORKFLOW=build-branch
   GITHUB_WORKSPACE=/tmp/test
   source checkout.sh
}


