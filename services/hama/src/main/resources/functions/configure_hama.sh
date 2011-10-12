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
function configure_hama() {
  local OPTIND
  local OPTARG
  
  ROLES=$1
  shift
  
  # get parameters
  MASTER_HOST=
  ZOOKEEKER_QUORUM=
  PORT=
  HAMA_TAR_URL=
  while getopts "m:q:u:" OPTION; do
    case $OPTION in
    m)
      MASTER_HOST="$OPTARG"
      ;;
    q)
      ZOOKEEPER_QUORUM="$OPTARG"
      ;;
    u)
      HAMA_TAR_URL="$OPTARG"
      ;;
    esac
  done
  
  # assign default URL if no other given (optional)
  HAMA_TAR_URL=${HAMA_TAR_URL:-http://archive.apache.org/dist/incubator/hama/0.3-incubating/hama-0.3.0-incubating.tar.gz}
  # derive details from the URL
  HAMA_TAR_FILE=${HAMA_TAR_URL##*/}

  HAMA_VERSION=${HAMA_TAR_FILE%.tar.gz}

  HAMA_HOME=/usr/local/$HAMA_VERSION
  HAMA_CONF_DIR=$HAMA_HOME/conf

  case $CLOUD_PROVIDER in
  ec2 | aws-ec2 )
    MOUNT=/mnt
    ;;
  *)
    MOUNT=/data
    ;;
  esac

  mkdir -p $MOUNT/hama
  chown hadoop:hadoop $MOUNT/hama
  if [ ! -e $MOUNT/tmp ]; then
    mkdir $MOUNT/tmp
    chmod a+rwxt $MOUNT/tmp
  fi
  mkdir /etc/hama
  ln -s $HAMA_CONF_DIR /etc/hama/conf

  ##############################################################################
  # Modify this section to customize your Hama cluster.
  ##############################################################################
  cat > $HAMA_CONF_DIR/hama-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>bsp.master.address</name>
  <value>$MASTER_HOST:40000</value>
</property>
<property>
 <name>fs.default.name</name>
 <value>hdfs://$MASTER_HOST:8020/</value>
</property>
<property>
 <name>hama.zookeeper.quorum</name>
 <value>$ZOOKEEPER_QUORUM</value>
</property>
<property>
 <name>hama.zookeeper.property.clientPort</name>
 <value>2181</value>
</property>
</configuration>
EOF

  # override JVM options
  cat >> $HAMA_CONF_DIR/hama-env.sh <<EOF
export HAMA_MASTER_OPTS="-Xms1000m -Xmx1000m -Xmn256m -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/mnt/hama/logs/hama-master-gc.log"
export HAMA_GROOMSERVER_OPTS="-Xms1000m -Xmx1000m -Xmn256m -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=88 -XX:+AggressiveOpts -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/mnt/hama/logs/hama-groomserver-gc.log"
EOF

  # keep PID files in a non-temporary directory
  sed -i -e "s|# export HAMA_PID_DIR=.*|export HAMA_PID_DIR=/var/run/hama|" \
    $HAMA_CONF_DIR/hama-env.sh
  mkdir -p /var/run/hama
  chown -R hadoop:hadoop /var/run/hama

  # set SSH options within the cluster
  sed -i -e 's|# export HAMA_SSH_OPTS=.*|export HAMA_SSH_OPTS="-o StrictHostKeyChecking=no"|' \
    $HAMA_CONF_DIR/hama-env.sh

  # disable IPv6
  sed -i -e 's|# export HAMA_OPTS=.*|export HAMA_OPTS="-Djava.net.preferIPv4Stack=true"|' \
    $HAMA_CONF_DIR/hama-env.sh

  # hama logs should be on the /mnt partition
  sed -i -e 's|# export HAMA_LOG_DIR=.*|export HAMA_LOG_DIR=/var/log/hama/logs|' \
    $HAMA_CONF_DIR/hama-env.sh
  rm -rf /var/log/hama
  mkdir $MOUNT/hama/logs
  chown hadoop:hadoop $MOUNT/hama/logs
  chown -R hadoop:hadoop $HAMA_HOME
  ln -s $MOUNT/hama/logs /var/log/hama
  chown -R hadoop:hadoop /var/log/hama
}
