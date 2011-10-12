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
  if which dpkg &> /dev/null; then
    cat > /etc/apt/sources.list.d/cloudera.list <<EOF
deb http://archive.cloudera.com/debian lucid-$REPO contrib
deb-src http://archive.cloudera.com/debian lucid-$REPO contrib
EOF
    curl -s http://archive.cloudera.com/debian/archive.key | sudo apt-key add -
    sudo apt-get update
  elif which rpm &> /dev/null; then
    rm -f /etc/yum.repos.d/cloudera.repo
    REPO_NUMBER=`echo $REPO | sed -e 's/cdh\([0-9][0-9]*\)/\1/'`
    cat > /etc/yum.repos.d/cloudera-$REPO.repo <<EOF
[cloudera-$REPO]
name=Cloudera's Distribution for Hadoop, Version $REPO_NUMBER
mirrorlist=http://archive.cloudera.com/redhat/cdh/$REPO_NUMBER/mirrors
gpgkey = http://archive.cloudera.com/redhat/cdh/RPM-GPG-KEY-cloudera
gpgcheck = 0
EOF
    yum update -y yum
  fi
}

function install_cdh_zookeeper() {
  local OPTIND
  local OPTARG
  
  case $CLOUD_PROVIDER in
    ec2 | aws-ec2 )
      # Alias /mnt as /data
      if [ ! -e /data ]; then ln -s /mnt /data; fi
      ;;
    *)
      ;;
  esac
  
  REPO=${REPO:-cdh3}
  ZOOKEEPER_HOME=/usr/lib/zookeeper
  ZK_CONF_DIR=/etc/zookeeper
  ZK_LOG_DIR=/var/log/zookeeper
  ZK_DATA_DIR=$ZK_LOG_DIR/txlog
  
  register_cloudera_repo
  
  if which dpkg &> /dev/null; then
    apt-get update
    apt-get -y install hadoop-zookeeper
  elif which rpm &> /dev/null; then
    yum install -y hadoop-zookeeper
  fi
  
  echo "export ZOOKEEPER_HOME=$ZOOKEEPER_HOME" >> /etc/profile
  echo 'export PATH=$ZOOKEEPER_HOME/bin:$PATH' >> /etc/profile
  
  rm -rf $ZK_LOG_DIR
  mkdir -p /data/zookeeper/logs
  ln -s /data/zookeeper/logs $ZK_LOG_DIR
  mkdir -p $ZK_LOG_DIR/txlog
  chown -R zookeeper:zookeeper /data/zookeeper/logs
  chown -R zookeeper:zookeeper $ZK_LOG_DIR
  
  sed -i -e "s|zookeeper.root.logger=.*|zookeeper.root.logger=INFO, ROLLINGFILE|" \
         -e "s|zookeeper.log.dir=.*|zookeeper.log.dir=$ZK_LOG_DIR|" \
         -e "s|zookeeper.tracelog.dir=.*|zookeeper.tracelog.dir=$ZK_LOG_DIR|" \
      $ZK_CONF_DIR/log4j.properties
}
