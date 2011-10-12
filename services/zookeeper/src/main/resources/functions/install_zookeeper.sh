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
function install_zookeeper() {
  local OPTIND
  local OPTARG
  
  ZK_TARBALL_URL=
  while getopts "u:" OPTION; do
    case $OPTION in
    u)
      ZK_TARBALL_URL="$OPTARG"
      ;;
    esac
  done
  
  case $CLOUD_PROVIDER in
    ec2 | aws-ec2 )
      # Alias /mnt as /data
      ln -s /mnt /data
      ;;
    *)
      ;;
  esac

  ZOOKEEPER_HOME=/usr/local/$(basename $ZK_TARBALL_URL '.tar.gz')

  ZK_CONF_DIR=/etc/zookeeper/conf
  ZK_LOG_DIR=/var/log/zookeeper
  ZK_DATA_DIR=$ZK_LOG_DIR/txlog
  
  install_tarball $ZK_TARBALL_URL
  
  echo "export ZOOKEEPER_HOME=$ZOOKEEPER_HOME" >> /etc/profile
  echo 'export PATH=$ZOOKEEPER_HOME/bin:$PATH' >> /etc/profile
  
  mkdir -p /mnt/zookeeper/logs
  ln -s /mnt/zookeeper/logs $ZK_LOG_DIR

  mkdir -p $ZK_LOG_DIR/txlog
  mkdir -p $ZK_CONF_DIR

  cp $ZOOKEEPER_HOME/conf/log4j.properties $ZK_CONF_DIR
  
  sed -i -e "s|log4j.rootLogger=INFO, CONSOLE|log4j.rootLogger=INFO, ROLLINGFILE|" \
         -e "s|log4j.appender.ROLLINGFILE.File=zookeeper.log|log4j.appender.ROLLINGFILE.File=$ZK_LOG_DIR/zookeeper.log|" \
      $ZK_CONF_DIR/log4j.properties
  
  # Install a CRON task for data directory cleanup
  ZK_JAR=$ZOOKEEPER_HOME/$(basename $ZK_TARBALL_URL '.tar.gz').jar
  ZK_LOG4J_JAR=`echo $ZOOKEEPER_HOME/lib/log4j-*.jar`
  
  CRON="0 0 * * * java -cp $ZK_JAR:$ZK_LOG4J_JAR:$ZK_CONF_DIR org.apache.zookeeper.server.PurgeTxnLog $ZK_DATA_DIR $ZK_DATA_DIR -n 10"
  crontab -l 2>/dev/null | { cat; echo "$CRON"; } | sort | uniq | crontab -

  cat >/etc/init.d/zookeeper <<EOF
#!/bin/bash
ZOOCFGDIR=$ZK_CONF_DIR $ZOOKEEPER_HOME/bin/zkServer.sh \$@ > /dev/null 2>&1 &
EOF
  chmod +x /etc/init.d/zookeeper
  install_service zookeeper
}
