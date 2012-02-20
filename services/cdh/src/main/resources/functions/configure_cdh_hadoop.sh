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
function configure_cdh_hadoop() {
  local OPTIND
  local OPTARG
  
  if [ "$CONFIGURE_HADOOP_DONE" == "1" ]; then
    echo "Hadoop is already configured."
    return;
  fi
  
  ROLES=$1
  shift
  
  case $CLOUD_PROVIDER in
    ec2 | aws-ec2 )
      # Alias /mnt as /data
      if [ ! -e /data ]; then ln -s /mnt /data; fi
      ;;
    *)
      ;;
  esac
  
  REPO=${REPO:-cdh3}
  HADOOP=hadoop-${HADOOP_VERSION:-0.20}
  HADOOP_CONF_DIR=/etc/$HADOOP/conf.dist
  
  mkdir -p /data/hadoop
  chgrp hadoop /data/hadoop
  chmod g+w /data/hadoop
  mkdir /data/tmp
  chmod a+rwxt /data/tmp

  # Copy generated configuration files in place
  cp /tmp/{core,hdfs,mapred}-site.xml $HADOOP_CONF_DIR
  cp /tmp/hadoop-env.sh $HADOOP_CONF_DIR

  # Expose /metrics URL endpoint
  cat > $HADOOP_CONF_DIR/hadoop-metrics.properties <<EOF
# Exposes /metrics URL endpoint for metrics information.
dfs.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
mapred.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
jvm.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
rpc.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
EOF

  # Keep PID files in a non-temporary directory
  HADOOP_PID_DIR=$(. /tmp/hadoop-env.sh; echo $HADOOP_PID_DIR)
  HADOOP_PID_DIR=${HADOOP_PID_DIR:-/var/run/hadoop}
  mkdir -p $HADOOP_PID_DIR
  chgrp -R hadoop $HADOOP_PID_DIR
  chmod -R g+w $HADOOP_PID_DIR

  # Create the actual log dir
  mkdir -p /data/hadoop/logs
  chgrp -R hadoop /data/hadoop/logs
  chmod -R g+w /data/hadoop/logs

  # Create a symlink at $HADOOP_LOG_DIR
  HADOOP_LOG_DIR=$(. /tmp/hadoop-env.sh; echo $HADOOP_LOG_DIR)
  HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-/var/log/hadoop/logs}
  rm -rf $HADOOP_LOG_DIR
  mkdir -p $(dirname $HADOOP_LOG_DIR)
  ln -s /data/hadoop/logs $HADOOP_LOG_DIR
  chgrp -R hadoop $HADOOP_LOG_DIR
  chmod -R g+w $HADOOP_LOG_DIR

  for role in $(echo "$ROLES" | tr "," "\n"); do
    case $role in
    hadoop-namenode)
      start_namenode
      ;;
    hadoop-secondarynamenode)
      start_hadoop_daemon secondarynamenode
      ;;
    hadoop-jobtracker)
      start_hadoop_daemon jobtracker
      ;;
    hadoop-datanode)
      start_hadoop_daemon datanode
      ;;
    hadoop-tasktracker)
      start_hadoop_daemon tasktracker
      ;;
    esac
  done
  
    CONFIGURE_HADOOP_DONE=1
  
}

function start_namenode() {
  if which dpkg &> /dev/null; then
    apt-get -y install $HADOOP-namenode
    AS_HDFS="su -s /bin/bash - hdfs -c"
    # Format HDFS
    [ ! -e /data/hadoop/hdfs ] && $AS_HDFS "$HADOOP namenode -format"
  elif which rpm &> /dev/null; then
    yum install -y $HADOOP-namenode
    AS_HDFS="/sbin/runuser -s /bin/bash - hdfs -c"
    # Format HDFS
    [ ! -e /data/hadoop/hdfs ] && $AS_HDFS "$HADOOP namenode -format"
  fi

  service $HADOOP-namenode start

  $AS_HDFS "$HADOOP dfsadmin -safemode wait"
  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /user"
  # The following is questionable, as it allows a user to delete another user
  # It's needed to allow users to create their own user directories
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /user"
  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /hadoop"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /hadoop"
  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /hbase"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /hbase"
  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /mnt"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /mnt"

  # Create temporary directory for Pig and Hive in HDFS
  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /tmp"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /tmp"
  $AS_HDFS "/usr/bin/$HADOOP fs -mkdir /user/hive/warehouse"
  $AS_HDFS "/usr/bin/$HADOOP fs -chmod +w /user/hive/warehouse"
}

function start_hadoop_daemon() {
  daemon=$1
  if which dpkg &> /dev/null; then
    apt-get -y install $HADOOP-$daemon
  elif which rpm &> /dev/null; then
    yum install -y $HADOOP-$daemon
  fi
  service $HADOOP-$daemon start
}

