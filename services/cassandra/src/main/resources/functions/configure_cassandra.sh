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
function configure_cassandra() {
  local OPTIND
  local OPTARG
  
  . /etc/profile

  OH_SIX_CONFIG="/etc/cassandra/conf/storage-conf.xml"
  
  if [[ -e "$OH_SIX_CONFIG" ]] ; then 
    config_file=$OH_SIX_CONFIG
    seeds=""
    for server in "$@"; do
      seeds="${seeds}<Seed>${server}</Seed>"
    done
  
    #TODO set replication
    sed -i -e "s|<Seed>127.0.0.1</Seed>|$seeds|" $config_file
    sed -i -e "s|<ListenAddress>localhost</ListenAddress>|<ListenAddress>$PRIVATE_IP</ListenAddress>|" $config_file
    sed -i -e "s|<ThriftAddress>localhost</ThriftAddress>|<ThriftAddress>$PUBLIC_IP</ThriftAddress>|" $config_file
  else
    config_file="/etc/cassandra/conf/cassandra.yaml"
    if [[ "x"`grep -e '^seeds:' $config_file` == "x" ]]; then
      seeds="$1" # 08 format seeds
      shift
      for server in "$@"; do
        seeds="${seeds},${server}"
      done
      sed -i -e "s|- seeds: \"127.0.0.1\"|- seeds: \"${seeds}\"|" $config_file
    else
      seeds="" # 07 format seeds
      for server in "$@"; do
        seeds="${seeds}\n    - ${server}"
      done
      sed -i -e "/^seeds:/,/^/d" $config_file ; echo -e "seeds:${seeds}" >> $config_file
    fi
  
    sed -i -e "s|listen_address: localhost|listen_address: $PRIVATE_IP|" $config_file
    sed -i -e "s|rpc_address: localhost|rpc_address: $PUBLIC_IP|" $config_file
  fi
}

