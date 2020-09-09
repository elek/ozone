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
Put and get file
                        Execute                    echo "Test content $(date)" > /tmp/sourcefile
    ${result} =         Execute                    hdfs dfs -mkdir s3a://${BUCKET}/${PREFIX}
    ${result} =         Execute                    hdfs dfs -put /tmp/sourcefile s3a://${BUCKET}/${PREFIX}/testfile
    ${result} =         Execute                    hdfs dfs -ls s3a://${BUCKET}/${PREFIX}/
                        Should contain             ${result}         s3a://${BUCKET}/${PREFIX}/testfile
    ${result} =         Execute                    hdfs dfs -get s3a://${BUCKET}/${PREFIX}/testfile /tmp/destination
    ${expected} =       Execute                    cat /tmp/sourcefile
    ${content} =        Execute                    cat /tmp/destination
                        Should Be Equal            ${expected}              ${content}

Create file and directory with the same name
                        Execute                    echo "Test content" > /tmp/conflict
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/conflict/dir1 --body /tmp/conflict
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/conflict/dir1/file1 --body /tmp/conflict
    ${result} =         Execute                    hdfs dfs -ls s3a://${BUCKET}/${PREFIX}/conflict/ | grep '\\-rw'
                        Should contain             ${result}       dir1
    ${result} =         Execute                    hdfs dfs -ls s3a://${BUCKET}/${PREFIX}/conflict/ | grep drw
                        Should contain             ${result}       dir1

Create files with path-incompatile names
                        Execute                    echo "Test content" > /tmp/conflict
                        Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/inc/file1 --body /tmp/conflict
                        Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/inc/dir1/dir2/file2 --body /tmp/conflict
                        Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/inc/dir1/../file3 --body /tmp/conflict
                        Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/inc////file4 --body /tmp/conflict
                        Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/inc/./file5 --body /tmp/conflict
    ${result} =         Execute                    hdfs dfs -ls s3a://${BUCKET}/${PREFIX}/inc
                        Should contain             ${result}       file1
                        Should contain             ${result}       dir1
                        Should not contain         ${result}       file3
                        Should not contain         ${result}       file4
                        Should not contain         ${result}       file5
    ${result} =         Execute                    hdfs dfs -ls s3a://${BUCKET}/${PREFIX}/inc/dir1
                        Should contain             ${result}       dir2
                        Should not contain         ${result}       file3
