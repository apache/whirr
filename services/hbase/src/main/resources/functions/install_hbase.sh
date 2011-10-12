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
function update_repo() {
  if which dpkg &> /dev/null; then
    sudo apt-get update
  elif which rpm &> /dev/null; then
    yum update -y yum
  fi
}

function install_hbase() {
  local OPTIND
  local OPTARG
  
  HBASE_TAR_URL=
  while getopts "u:" OPTION; do
    case $OPTION in
    u)
      HBASE_TAR_URL="$OPTARG"
      ;;
    esac
  done
  
  # assign default URL if no other given (optional)
  HBASE_TAR_URL=${HBASE_TAR_URL:-http://archive.apache.org/dist/hbase/hbase-0.90.0/hbase-0.90.0.tar.gz}
  # derive details from the URL
  HBASE_TAR_FILE=${HBASE_TAR_URL##*/}
  HBASE_TAR_MD5_FILE=$HBASE_TAR_FILE.md5
  # extract "version" or the name of the directory contained in the tarball,
  # but since hbase has used different namings use the directory instead.
  HBASE_VERSION=${HBASE_TAR_URL%/*.tar.gz}
  HBASE_VERSION=${HBASE_VERSION##*/}
  # simple check that we have a proper URL or default to use filename
  if [[ "${HBASE_VERSION:0:5}" != "hbase" ]]; then
    HBASE_VERSION=${HBASE_TAR_FILE%.tar.gz}
  fi
  HBASE_HOME=/usr/local/$HBASE_VERSION
  HBASE_CONF_DIR=$HBASE_HOME/conf

  update_repo

  if ! id hadoop &> /dev/null; then
    useradd hadoop
  fi

  # up file-max
  sysctl -w fs.file-max=65535
  # up ulimits
  echo "root soft nofile 65535" >> /etc/security/limits.conf
  echo "root hard nofile 65535" >> /etc/security/limits.conf
  ulimit -n 65535
  # up epoll limits; ok if this fails, only valid for kernels 2.6.27+
  set +e
  sysctl -w fs.epoll.max_user_instances=4096 > /dev/null 2>&1
  set -e
  # if there is no hosts file then provide a minimal one
  [ ! -f /etc/hosts ] && echo "127.0.0.1 localhost" > /etc/hosts

  install_tarball $HBASE_TAR_URL

  echo "export HBASE_HOME=$HBASE_HOME" >> ~root/.bashrc
  echo 'export PATH=$JAVA_HOME/bin:$HBASE_HOME/bin:$PATH' >> ~root/.bashrc
}

