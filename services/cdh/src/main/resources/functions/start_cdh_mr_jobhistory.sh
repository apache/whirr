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
function start_cdh_mr_jobhistory() {
  if which dpkg &> /dev/null; then
    retry_apt_get -y install hadoop-mapreduce-historyserver
  elif which rpm &> /dev/null; then
    retry_yum install -y hadoop-mapreduce-historyserver
  fi
  service hadoop-mapreduce-historyserver start
}
