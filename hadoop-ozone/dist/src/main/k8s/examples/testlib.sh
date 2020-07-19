#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

retry() {
   n=0
   until [ $n -ge 30 ]
   do
      "$@" && break
      n=$[$n+1]
      echo "$n '$@' is failed..."
      sleep 3
   done
}

grep_log() {
   CONTAINER="$1"
   PATTERN="$2"
   kubectl logs "$1"  | grep "$PATTERN"
}

wait_for_startup(){
   retry all_pods_are_running
   retry grep_log scm-0 "SCM exiting safe mode."
   retry grep_log om-0 "HTTP server of ozoneManager listening"
}

all_pods_are_running() {
   RUNNING_COUNT=$(kubectl get pod --field-selector status.phase=Running | wc -l)
   ALL_COUNT=$(kubectl get pod | wc -l)
   RUNNING_COUNT=$((RUNNING_COUNT - 1))
   ALL_COUNT=$((ALL_COUNT - 1))
   if [ "$RUNNING_COUNT" -lt "3" ]; then
      echo "$RUNNING_COUNT pods are running. Waiting for more."
      return 1
   elif [ "$RUNNING_COUNT" -ne "$ALL_COUNT" ]; then
      echo "$RUNNING_COUNT pods are running out from the $ALL_COUNT"
      return 2
   else
      STARTED=true
      return 0
   fi
}

start_k8s_env() {
   #reset environment
   kubectl delete statefulset --all
   kubectl delete daemonset --all
   kubectl delete deployment --all
   kubectl delete service --all
   kubectl delete configmap --all
   kubectl delete pod --all

   kubectl apply -f "$1"
   wait_for_startup
}

stop_k8s_env() {
   if [ ! "$KEEP_RUNNING" ]; then
     kubectl delete -f "$1"
   fi
}

regenerate_resources() {
  cd $1

  PARENT_OF_PARENT=$(realpath "$1"/../..)

  if [ $(basename $PARENT_OF_PARENT) == "k8s" ]; then
    #running from src dir
    OZONE_ROOT=$(realpath "$1"/../../../../../target/ozone-0.6.0-SNAPSHOT)
  else
    #running from dist
    OZONE_ROOT=$(realpath "$1"/../../..)
  fi

  flekszible generate -t mount:hostPath="$OZONE_ROOT",path=/opt/hadoop -t image:image=apache/ozone-runner:20200420-1 -t ozone/onenode
  cd -

}

execute_robot_test() {
   OUTPUT_DIR="$1"
   mkdir -p "$OUTPUT_DIR/result"
   shift 1 #Remove first argument which was the container name

   CONTAINER="$1"
   shift 1 #Remove first argument which was the container name

   # shellcheck disable=SC2206
   ARGUMENTS=($@)

   kubectl exec -it "${CONTAINER}" -- bash -c 'rm -rf /tmp/report'
   kubectl exec -it "${CONTAINER}" -- bash -c 'mkdir -p  /tmp/report'
   kubectl exec -it "${CONTAINER}" -- robot -d /tmp/report ${ARGUMENTS[@]} || true
   kubectl cp "${CONTAINER}":/tmp/report/output.xml "$OUTPUT_DIR/result/$CONTAINER-$RANDOM.xml" || true
}

combine_reports() {
  rm "$1"/result/output.xml || true
  rebot -d "$1"/result -o "$1"/result/output.xml -N $(basename $1) "$1"/result/*.xml
}
