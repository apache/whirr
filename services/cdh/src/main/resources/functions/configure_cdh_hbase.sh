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
  CLOUD_PROVIDER=
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
    c)
      CLOUD_PROVIDER="$OPTARG"
      ;;
    u)
      HBASE_TAR_URL="$OPTARG"
      ;;
    esac
  done
  
  # determine machine name
  case $CLOUD_PROVIDER in
    ec2 | aws-ec2 )
      # Use public hostname for EC2
      SELF_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/public-hostname`
      ;;
    *)
      SELF_HOST=`/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
      ;;
  esac
  
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

  # Copy generated configuration file in place
  cp /tmp/hbase-site.xml $HBASE_CONF_DIR

  # override JVM options
  cat >> $HBASE_CONF_DIR/hbase-env.sh <<EOF
export HBASE_MASTER_OPTS="-Xms1000m -Xmx1000m -Xmn256m -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/data/hbase/logs/hbase-master-gc.log"
export HBASE_REGIONSERVER_OPTS="-Xms2000m -Xmx2000m -Xmn256m -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=88 -XX:+AggressiveOpts -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/data/hbase/logs/hbase-regionserver-gc.log"
EOF

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

  # keep PID files in a non-temporary directory
  sed -i -e "s|# export HBASE_PID_DIR=.*|export HBASE_PID_DIR=/var/run/hbase|" \
    $HBASE_CONF_DIR/hbase-env.sh

  # set SSH options within the cluster
  sed -i -e 's|# export HBASE_SSH_OPTS=.*|export HBASE_SSH_OPTS="-o StrictHostKeyChecking=no"|' \
    $HBASE_CONF_DIR/hbase-env.sh

  # disable IPv6
  sed -i -e 's|export HBASE_OPTS="$HBASE_OPTS -ea -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode"|export HBASE_OPTS="$HBASE_OPTS -ea -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.net.preferIPv4Stack=true"|' \
    $HBASE_CONF_DIR/hbase-env.sh

  # hbase logs should be on the /data partition
  sed -i -e 's|# export HBASE_LOG_DIR=.*|export HBASE_LOG_DIR=/var/log/hbase/logs|' \
    $HBASE_CONF_DIR/hbase-env.sh
  rm -rf /var/log/hbase
  mkdir /data/hbase/logs
  chown hbase:hbase /data/hbase/logs
  ln -s /data/hbase/logs /var/log/hbase
  chown -R hbase:hbase /var/log/hbase

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
