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

*** Settings ***
Documentation       S3a test with Ozone
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Keywords ***
Create Dest Bucket

    ${postfix} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable   ${DESTBUCKET}             destbucket-${postfix}
    Execute AWSS3APICli  create-bucket --bucket ${DESTBUCKET}


*** Test Cases ***
#Key with double slash
#                        Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/comp//file1 --body /tmp/conflict
#     ${result} =        Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/comp
#                        Should contain             ${result}       ${PREFIX}/comp//file1
#     ${result} =        Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/comp
#                        Should contain             ${result}       PRE comp/
#     ${result} =        Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/comp/
#                        Should contain             ${result}       PRE /
#

#Key with dot
#                        Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/comp/./file1 --body /tmp/conflict
#     ${result} =        Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/comp
#                        Should contain             ${result}       ${PREFIX}/comp/./file1
#     ${result} =        Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/comp
#                        Should contain             ${result}       PRE comp/
#     ${result} =                    ls s3://${BUCKET}/${PREFIX}/comp/
#                        Should contain             ${result}       PRE ./

Key with dotdot
                        Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/comp/../file1 --body /tmp/conflict
     ${result} =        Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/comp
                        Should contain             ${result}       ${PREFIX}/comp//file1
     ${result} =        Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/comp
                        Should contain             ${result}       PRE comp/
     ${result} =        Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/comp/
                        Should contain             ${result}       PRE ..
