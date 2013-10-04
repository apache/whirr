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
function configure_druid() {

  ROLE=$1
  ZOOKEEPER_QUORUM=$2
  PORT=$3
  MYSQL_HOSTNAME=$4
  IDENTITY=$5
  CREDENTIAL=$6
  S3_BUCKET=$7
  HOSTNAME=$PRIVATE_IP
  ROLE_NAME=${ROLE/druid-//}

  echo "ROLE: $ROLE, ZOOKEEPER_QUORUM: $ZOOKEEPER_QUORUM, PORT: $PORT, MYSQL_HOSTNAME=$MYSQL_HOSTNAME, HOSTNAME=$HOSTNAME, ROLE_NAME=$ROLE_NAME"

  # Configure runtime.properties with Zookeeper address
  cat > /usr/local/druid-services-0.5.7/config/$ROLE_NAME/runtime.properties <<EOF

# Druid base config
com.metamx.emitter.logging=true

druid.processing.formatString=processing_%s
druid.processing.numThreads=1
druid.processing.buffer.sizeBytes=10000000

#emitting, opaque marker
druid.service=example

druid.request.logging.dir=/tmp/example/log
druid.realtime.specFile=realtime.spec
com.metamx.emitter.logging=true
com.metamx.emitter.logging.level=info

# below are dummy values when operating a realtime only node
com.metamx.aws.accessKey=$IDENTITY
com.metamx.aws.secretKey=$CREDENTIAL
druid.pusher.s3.bucket=${S3_BUCKET}

druid.client.http.connections=30
druid.zk.service.host=$ZOOKEEPER_QUORUM
druid.server.maxSize=300000000000
druid.zk.paths.base=/druid
druid.database.segmentTable=prod_segments
druid.database.user=druid
druid.database.password=diurd
druid.database.connectURI=jdbc:mysql://$MYSQL_HOSTNAME:3306/druid
druid.zk.paths.discoveryPath=/druid/discoveryPath
druid.database.ruleTable=rules
druid.database.configTable=config

# Path on local FS for storage of segments; dir will be created if needed
druid.paths.indexCache=/tmp/druid/indexCache
# Path on local FS for storage of segment metadata; dir will be created if needed
druid.paths.segmentInfoCache=/tmp/druid/segmentInfoCache
druid.pusher.local.storageDirectory=/tmp/druid/localStorage
druid.pusher.local=true

druid.host=$HOSTNAME:$PORT
druid.port=$PORT
EOF

}