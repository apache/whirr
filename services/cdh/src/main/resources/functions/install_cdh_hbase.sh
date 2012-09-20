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
function register_cloudera_repo() {
  CDH_MAJOR_VERSION=$(echo $REPO | sed -e 's/cdh\([0-9]\).*/\1/')
  CDH_VERSION=$(echo $REPO | sed -e 's/cdh\([0-9][0-9]*\)/\1/')
  if which dpkg &> /dev/null; then
    if [ $CDH_MAJOR_VERSION = "4" ]; then
      cat > /etc/apt/sources.list.d/cloudera-cdh4.list <<EOF
deb http://$REPO_HOST/cdh4/ubuntu/lucid/amd64/cdh lucid-cdh4 contrib
deb-src http://$REPO_HOST/cdh4/ubuntu/lucid/amd64/cdh lucid-cdh4 contrib
EOF
      curl -s http://$REPO_HOST/cdh4/ubuntu/lucid/amd64/cdh/archive.key | apt-key add -
    else
      cat > /etc/apt/sources.list.d/cloudera-$REPO.list <<EOF
deb http://$REPO_HOST/debian lucid-$REPO contrib
deb-src http://$REPO_HOST/debian lucid-$REPO contrib
EOF
      curl -s http://$REPO_HOST/debian/archive.key | apt-key add -
    fi
    retry_apt_get -y update
  elif which rpm &> /dev/null; then
    if [ $CDH_MAJOR_VERSION = "4" ]; then
      cat > /etc/yum.repos.d/cloudera-cdh4.repo <<EOF
[cloudera-cdh4]
name=Cloudera's Distribution for Hadoop, Version 4
baseurl=http://$REPO_HOST/cdh4/redhat/5/x86_64/cdh/4/
http://repos.jenkins.sf.cloudera.com/cdh4-nightly/redhat/5/x86_64/cdh/4/
gpgkey = http://$REPO_HOST/cdh4/redhat/5/x86_64/cdh/RPM-GPG-KEY-cloudera 
gpgcheck = 1
EOF
    else
      cat > /etc/yum.repos.d/cloudera-$REPO.repo <<EOF
[cloudera-$REPO]
name=Cloudera's Distribution for Hadoop, Version $CDH_VERSION
mirrorlist=http://$REPO_HOST/redhat/cdh/$CDH_VERSION/mirrors
gpgkey = http://$REPO_HOST/redhat/cdh/RPM-GPG-KEY-cloudera
gpgcheck = 0
EOF
    fi
    retry_yum update -y retry_yum
  fi
}

function install_cdh_hbase() {
  local OPTIND
  local OPTARG
  
  HBASE_TAR_URL=
  while getopts "u:" OPTION; do
    case $OPTION in
    u)
      # ignore tarball
      ;;
    esac
  done
  
  REPO=${REPO:-cdh4}
  REPO_HOST=${REPO_HOST:-archive.cloudera.com}
  HBASE_HOME=/usr/lib/hbase
  
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

  register_cloudera_repo
  CDH_MAJOR_VERSION=$(echo $REPO | sed -e 's/cdh\([0-9]\).*/\1/')
  if [ $CDH_MAJOR_VERSION = "4" ]; then
    HBASE_PREFIX=
  else
    HBASE_PREFIX=hadoop-
  fi
  
  if which dpkg &> /dev/null; then
    retry_apt_get update
    retry_apt_get -y install ${HBASE_PREFIX}hbase
  elif which rpm &> /dev/null; then
    retry_yum install -y ${HBASE_PREFIX}hbase
  fi
  
  echo "export HBASE_HOME=$HBASE_HOME" >> ~root/.bashrc
  echo 'export PATH=$JAVA_HOME/bin:$HBASE_HOME/bin:$PATH' >> ~root/.bashrc
}
