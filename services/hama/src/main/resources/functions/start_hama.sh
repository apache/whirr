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
set -x
function start_hama() {
  local OPTIND
  local OPTARG
  
  ROLES=$1
  shift
  
  # get parameters
  PORT=
  HAMA_TAR_URL=
  while getopts "u:" OPTION; do
    case $OPTION in
    u)
      HAMA_TAR_URL="$OPTARG"
      ;;
    esac
  done

  # assign default URL if no other given (optional)
  HAMA_TAR_URL=${HAMA_TAR_URL:-http://archive.apache.org/dist/incubator/hama/0.3-incubating/hama-0.3.0-incubating.tar.gz}
  # derive details from the URL
  HAMA_TAR_FILE=${HAMA_TAR_URL##*/}

  HAMA_VERSION=${HAMA_TAR_FILE%.tar.gz}

  HAMA_HOME=/usr/local/$HAMA_VERSION
  HAMA_CONF_DIR=$HAMA_HOME/conf
  
  for role in $(echo "$ROLES" | tr "," "\n"); do
    case $role in
    hama-master)
      start_hama_daemon bspmaster
      ;;
    hama-groomserver)
      start_hama_daemon groom
      ;;
    esac
  done
}

function start_hama_daemon() {
  if which dpkg &> /dev/null; then
    AS_HADOOP="su -s /bin/bash - hadoop -c"
  elif which rpm &> /dev/null; then
    AS_HADOOP="/sbin/runuser -s /bin/bash - hadoop -c"
  fi
  $AS_HADOOP "$HAMA_HOME/bin/hama-daemon.sh start $1" 
}
