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
function configure_cdh_hbase() {
  local OPTIND
  local OPTARG
  
  ROLES=$1
  shift
  
  # get parameters
  MASTER_HOST=
  ZOOKEEPER_QUORUM=
  PORT=
  HBASE_TAR_URL=
  while getopts "m:q:p:c:u:" OPTION; do
    case $OPTION in
    m)
      MASTER_HOST="$OPTARG"
      ;;
    q)
      ZOOKEEPER_QUORUM="$OPTARG"
      ;;
    p)
      PORT="$OPTARG"
      ;;
    u)
      HBASE_TAR_URL="$OPTARG"
      ;;
    esac
  done
  
  HBASE_HOME=/usr/lib/$HBASE_VERSION
  HBASE_CONF_DIR=/etc/hbase/conf

  case $CLOUD_PROVIDER in
    ec2 | aws-ec2 )
      # Alias /mnt as /data
      if [ ! -e /data ]; then ln -s /mnt /data; fi
      ;;
    *)
      ;;
  esac

  mkdir -p /data/hbase
  chown hbase:hbase /data/hbase
  if [ ! -e /data/tmp ]; then
    mkdir /data/tmp
    chmod a+rwxt /data/tmp
  fi

  # Copy generated configuration files in place
  cp /tmp/hbase-site.xml $HBASE_CONF_DIR
  cp /tmp/hbase-env.sh $HBASE_CONF_DIR

  # HBASE_PID_DIR should exist and be owned by hadoop:hadoop
  HBASE_PID_DIR=$(. $HBASE_CONF_DIR/hbase-env.sh; echo $HBASE_PID_DIR)
  HBASE_PID_DIR=${HBASE_PID_DIR:-/var/run/hbase}
  mkdir -p $HBASE_PID_DIR
  chown -R hadoop:hadoop $HBASE_PID_DIR

  # Create the actual log dir
  mkdir -p /data/hbase/logs
  chown -R hbase:hbase /data/hbase/logs

  # Create a symlink at $HBASE_LOG_DIR
  HBASE_LOG_DIR=$(. $HBASE_CONF_DIR/hbase-env.sh; echo $HBASE_LOG_DIR)
  HBASE_LOG_DIR=${HBASE_LOG_DIR:-/var/log/hbase}
  rm -rf $HBASE_LOG_DIR
  mkdir -p $(dirname $HBASE_LOG_DIR)
  ln -s /data/hbase/logs $HBASE_LOG_DIR
  chown -R hbase:hbase $HBASE_LOG_DIR

  # configure hbase for ganglia
  cat > $HBASE_CONF_DIR/hadoop-metrics.properties <<EOF
dfs.class=org.apache.hadoop.metrics.ganglia.GangliaContext
dfs.period=10
dfs.servers=$MASTER_HOST:8649
hbase.class=org.apache.hadoop.metrics.ganglia.GangliaContext
hbase.period=10
hbase.servers=$MASTER_HOST:8649
jvm.class=org.apache.hadoop.metrics.ganglia.GangliaContext
jvm.period=10
jvm.servers=$MASTER_HOST:8649
EOF

  # Now that the configuration is done, install the daemon packages
  for role in $(echo "$ROLES" | tr "," "\n"); do
    case $role in
    hbase-master)
      install_hbase_daemon hadoop-hbase-master
      ;;
    hbase-regionserver)
      install_hbase_daemon hadoop-hbase-regionserver
      ;;
    hbase-restserver)
      # not supported
      ;;
    hbase-avroserver)
      # not supported
      ;;
    hbase-thriftserver)
      install_hbase_daemon hadoop-hbase-thrift
      ;;
    esac
  done

  # Start services
  # For DEB, the services have already been started as part of the daemon package installation
  if which rpm &> /dev/null; then
    for role in $(echo "$ROLES" | tr "," "\n"); do
      case $role in
      hbase-master)
        service hadoop-hbase-master restart
        ;;
      hbase-regionserver)
        service hadoop-hbase-regionserver restart
        ;;
      hbase-restserver)
        # not supported
        ;;
      hbase-avroserver)
        # not supported
        ;;
      hbase-thriftserver)
        service hadoop-hbase-thrift restart
        ;;
      esac
    done
  fi
}

function install_hbase_daemon() {
  daemon=$1
  if which dpkg &> /dev/null; then
    apt-get -y install $daemon
  elif which rpm &> /dev/null; then
    yum install -y $daemon
  fi
}
