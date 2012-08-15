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

  REPO=${REPO:-cdh4}
  CDH_MAJOR_VERSION=$(echo $REPO | sed -e 's/cdh\([0-9]\).*/\1/')
  if [ $CDH_MAJOR_VERSION = "4" ]; then
    HBASE_PREFIX=
  else
    HBASE_PREFIX=hadoop-
  fi

  make_hbase_dirs /data*

  # Copy generated configuration files in place
  cp /tmp/hbase-site.xml $HBASE_CONF_DIR
  cp /tmp/hbase-env.sh $HBASE_CONF_DIR
  cp /tmp/hbase-hadoop-metrics.properties $HBASE_CONF_DIR/hadoop-metrics.properties

  # HBASE_PID_DIR should exist and be owned by hbase:hbase
  HBASE_PID_DIR=$(. $HBASE_CONF_DIR/hbase-env.sh; echo $HBASE_PID_DIR)
  HBASE_PID_DIR=${HBASE_PID_DIR:-/var/run/hbase}
  mkdir -p $HBASE_PID_DIR
  chown -R hbase:hbase $HBASE_PID_DIR

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

  # Now that the configuration is done, install the daemon packages
  for role in $(echo "$ROLES" | tr "," "\n"); do
    case $role in
    hbase-master)
      install_hbase_daemon ${HBASE_PREFIX}hbase-master
      ;;
    hbase-regionserver)
      install_hbase_daemon ${HBASE_PREFIX}hbase-regionserver
      ;;
    hbase-restserver)
      # not supported
      ;;
    hbase-avroserver)
      # not supported
      ;;
    hbase-thriftserver)
      install_hbase_daemon ${HBASE_PREFIX}hbase-thrift
      ;;
    esac
  done

  # Start services
  # For DEB, the services have already been started as part of the daemon package installation
  if which rpm &> /dev/null; then
    for role in $(echo "$ROLES" | tr "," "\n"); do
      case $role in
      hbase-master)
        service ${HBASE_PREFIX}hbase-master restart
        ;;
      hbase-regionserver)
        service ${HBASE_PREFIX}hbase-regionserver restart
        ;;
      hbase-restserver)
        # not supported
        ;;
      hbase-avroserver)
        # not supported
        ;;
      hbase-thriftserver)
        service ${HBASE_PREFIX}hbase-thrift restart
        ;;
      esac
    done
  fi
}

function install_hbase_daemon() {
  daemon=$1
  if which dpkg &> /dev/null; then
    retry_apt_get -y install $daemon
  elif which rpm &> /dev/null; then
    retry_yum install -y $daemon
  fi
}


function make_hbase_dirs {
  for mount in "$@"; do
    if [ ! -e $mount/hbase ]; then
      mkdir -p $mount/hbase
      chown hbase:hbase $mount/hbase
    fi
    if [ ! -e $mount/tmp ]; then
      mkdir $mount/tmp
      chmod a+rwxt $mount/tmp
    fi
  done
}
