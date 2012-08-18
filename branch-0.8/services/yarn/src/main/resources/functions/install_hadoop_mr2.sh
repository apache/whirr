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

function update_repo() {
  if which dpkg &> /dev/null; then
    sudo apt-get update
  elif which rpm &> /dev/null; then
    yum update -y yum
  fi
}

function install_hadoop_mr2() {
  local OPTIND
  local OPTARG
  
  if [ "$INSTALL_HADOOP_DONE" == "1" ]; then
    echo "Hadoop is already installed."
    return;
  fi
  
  HADOOP_TAR_URL=
  while getopts "u:" OPTION; do
    case $OPTION in
    u)
      HADOOP_TAR_URL="$OPTARG"
      ;;
    esac
  done

  HADOOP_HOME=/usr/local/$(basename $HADOOP_TAR_URL .tar.gz)
  HADOOP_COMMON_HOME=$HADOOP_HOME
  HADOOP_HDFS_HOME=$HADOOP_HOME
  HADOOP_MAPRED_HOME=$HADOOP_HOME
  YARN_HOME=$HADOOP_HOME

  update_repo

  if ! id hadoop &> /dev/null; then
    useradd -m hadoop
  fi

  install_tarball $HADOOP_TAR_URL
  ln -s $HADOOP_HOME /usr/local/hadoop
  chown hadoop:hadoop $HADOOP_HOME
  
  cat >> /etc/profile <<EOF
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME
export HADOOP_HDFS_HOME=$HADOOP_HDFS_HOME
export HADOOP_MAPRED_HOME=$HADOOP_MAPRED_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_HOME=$YARN_HOME
export YARN_CONF_DIR=$YARN_HOME/conf
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
EOF

  INSTALL_HADOOP_DONE=1

}

