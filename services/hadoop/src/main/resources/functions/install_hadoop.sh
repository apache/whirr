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

function install_hadoop() {
  local OPTIND
  local OPTARG
  
  HADOOP_TAR_URL=
  while getopts "u:" OPTION; do
    case $OPTION in
    u)
      HADOOP_TAR_URL="$OPTARG"
      ;;
    esac
  done

  HADOOP_HOME=/usr/local/$(basename $HADOOP_TAR_URL .tar.gz)

  update_repo

  if ! id hadoop &> /dev/null; then
    useradd hadoop
  fi
  
  install_tarball $HADOOP_TAR_URL
  ln -s $HADOOP_HOME /usr/local/hadoop

  echo "export HADOOP_HOME=$HADOOP_HOME" >> ~root/.bashrc
  echo 'export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH' >> ~root/.bashrc
}

