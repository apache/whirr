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
function register_cloudera_repo() {
  CDH_MAJOR_VERSION=$(echo $REPO | sed -e 's/cdh\([0-9]\).*/\1/')
  CDH_VERSION=$(echo $REPO | sed -e 's/cdh\([0-9][0-9]*\)/\1/')
  if which dpkg &> /dev/null; then
    cat > /etc/apt/sources.list.d/cloudera.list <<EOF
deb http://archive.cloudera.com/debian lucid-$REPO contrib
deb-src http://archive.cloudera.com/debian lucid-$REPO contrib
EOF
    curl -s http://archive.cloudera.com/debian/archive.key | apt-key add -
    retry_apt_get -y update
  elif which rpm &> /dev/null; then
    rm -f /etc/yum.repos.d/cloudera*.repo
    if [ $CDH_MAJOR_VERSION = "4" ]; then
      cat > /etc/yum.repos.d/cloudera-cdh4.repo <<EOF
[cloudera-cdh4]
name=Cloudera's Distribution for Hadoop, Version 4
baseurl=http://archive.cloudera.com/cdh4/redhat/5/x86_64/cdh/4/
http://repos.jenkins.sf.cloudera.com/cdh4-nightly/redhat/5/x86_64/cdh/4/
gpgkey = http://archive.cloudera.com/cdh4/redhat/5/x86_64/cdh/RPM-GPG-KEY-cloudera 
gpgcheck = 1
EOF
    else
      cat > /etc/yum.repos.d/cloudera-$REPO.repo <<EOF
[cloudera-$REPO]
name=Cloudera's Distribution for Hadoop, Version $CDH_VERSION
mirrorlist=http://archive.cloudera.com/redhat/cdh/$CDH_VERSION/mirrors
gpgkey = http://archive.cloudera.com/redhat/cdh/RPM-GPG-KEY-cloudera
gpgcheck = 0
EOF
    fi
    retry_yum update -y yum
  fi
}

function install_cdh_hadoop() {
  local OPTIND
  local OPTARG

  REPO=${REPO:-cdh4}
  MAPREDUCE_VERSION=${MAPREDUCE_VERSION:-1}
  HADOOP=hadoop-${HADOOP_VERSION:-0.20}
  HADOOP_CONF_DIR=/etc/$HADOOP/conf.dist
  HADOOP_PACKAGE=hadoop-${HADOOP_VERSION:-0.20}
  
  CDH_MAJOR_VERSION=$(echo $REPO | sed -e 's/cdh\([0-9]\).*/\1/')
  if [ $CDH_MAJOR_VERSION = "4" ]; then
    HADOOP=hadoop
    HADOOP_CONF_DIR=/etc/$HADOOP/conf.dist
    if [ $MAPREDUCE_VERSION = "1" ]; then
      HADOOP_PACKAGE=hadoop-0.20-mapreduce
    else
      HADOOP_PACKAGE=hadoop-mapreduce
    fi
  fi

  register_cloudera_repo
  
  if which dpkg &> /dev/null; then
    apt-get update
    apt-get -y install $HADOOP_PACKAGE
    cp -r /etc/$HADOOP/conf.empty $HADOOP_CONF_DIR
    update-alternatives --install /etc/$HADOOP/conf $HADOOP-conf $HADOOP_CONF_DIR 90
  elif which rpm &> /dev/null; then
    yum install -y $HADOOP_PACKAGE
    cp -r /etc/$HADOOP/conf.empty $HADOOP_CONF_DIR
    alternatives --install /etc/$HADOOP/conf $HADOOP-conf $HADOOP_CONF_DIR 90
  fi
  
  INSTALL_HADOOP_DONE=1
}
