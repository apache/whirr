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
function configure_cdh_zookeeper() {
  local OPTIND
  local OPTARG
  
  myid_file=/var/log/zookeeper/txlog/myid
  config_file=/etc/zookeeper/zoo.cfg
  
  cat > $config_file <<EOF
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=5
# The directory where the snapshot is stored.
dataDir=/var/log/zookeeper/txlog
# The port at which the clients will connect
clientPort=2181
# The servers in the ensemble
EOF
  
  if [[ $# -gt 1 ]]; then
    id=1
    for server in "$@"; do
      if [ $server == $PRIVATE_IP ]; then
        myid=$id
      fi
      echo "server.$id=$server:2888:3888" >> $config_file
      id=$((id+1))
    done
  
    if [ -z $myid ]; then
      echo "Could not determine id for my host $PRIVATE_IP against servers $@."
      exit 1
    fi
    echo $myid > $myid_file
  fi
  
  # Now that it's configured, install daemon package
  if which dpkg &> /dev/null; then
    apt-get -y install hadoop-zookeeper-server
  elif which rpm &> /dev/null; then
    yum install -y hadoop-zookeeper-server
  fi

  # Start ZooKeeper
  # For DEB, the service is already started as part of the daemon package installation
  if which rpm &> /dev/null; then
    service hadoop-zookeeper-server start
  fi
}
