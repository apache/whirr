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

  ZK_VERSION=${1:-3.3.0}
  ZOOKEEPER_HOME=/usr/local/zookeeper-$ZK_VERSION
  ZK_CONF_DIR=/etc/zookeeper/conf
  ZK_LOG_DIR=/var/log/zookeeper
  ZK_DATA_DIR=$ZK_LOG_DIR/txlog
  
  install_tarball http://www.apache.org/dist/hadoop/zookeeper/zookeeper-$ZK_VERSION/zookeeper-$ZK_VERSION.tar.gz
  
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
  ZK_JAR=$ZOOKEEPER_HOME/zookeeper-$ZK_VERSION.jar
  ZK_LOG4J_JAR=`echo $ZOOKEEPER_HOME/lib/log4j-*.jar`
  
  CRON="0 0 * * * java -cp $ZK_JAR:$ZK_LOG4J_JAR:$ZK_CONF_DIR org.apache.zookeeper.server.PurgeTxnLog $ZK_DATA_DIR $ZK_DATA_DIR -n 10"
  crontab -l 2>/dev/null | { cat; echo "$CRON"; } | sort | uniq | crontab -
      
  # Ensure ZooKeeper starts on boot
  sed -i -e "s/exit 0//" /etc/rc.local
cat >> /etc/rc.local <<EOF
ZOOCFGDIR=$ZK_CONF_DIR $ZOOKEEPER_HOME/bin/zkServer.sh start > /dev/null 2>&1 &
EOF

}
