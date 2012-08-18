#
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

function retry_helpers() {
  echo "This function does nothing. It just needs to exist so Statements.call(\"retry_helpers\") doesn't call something which doesn't exist"
}

function retry() {
  tries=$1
  interval=$2
  expected_exit_code=$3
  shift 3

  while [ "$tries" -gt 0 ]; do
    $@
    last_exit_code=$?

    if [ "$last_exit_code" -eq "$expected_exit_code" ]; then
      break
    fi

    tries=$((tries-1))
    if [ "$tries" -gt 0 ]; then
      sleep $interval
    fi
  done
  # Ugly hack to avoid substitution (re_turn -> exit)
  "re""turn" $last_exit_code
}

function retry_apt_get() {
  retry 5 5 0 apt-get -o APT::Acquire::Retries=5 $@
}

function retry_yum() {
  retry 5 5 0 yum $@
}

