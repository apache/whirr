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
function install_elasticsearch() {
    local CURL="curl -L --silent --show-error --fail --connect-timeout 10 --max-time 600 --retry 5"

    local ES_URL=${1:-http://github.com/downloads/elasticsearch/elasticsearch/elasticsearch-0.15.2.tar.gz}
    local TAR_FILE=`basename $ES_URL`

    for retry_count in `seq 1 3`
    do
      $CURL -O $ES_URL || true
      if [ -f $TAR_FILE ]; then
        break;
      fi
      if [ ! $retry_count -eq "3" ]; then
        sleep 10
      fi
    done

    if [ -f $TAR_FILE ]; then
      tar xzf $TAR_FILE -C /usr/local
      rm -f $TAR_FILE
    else
      echo "Unable to download tar file from $ES_URL" >&2
      exit 1
    fi
}

