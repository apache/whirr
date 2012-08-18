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

# -c <cloud-provider> -p <number_of_partitions_per_node> -n <cluster_name> <server-ip>*                                             
function configure_voldemort() {
  . /etc/profile

  while getopts "p:" OPTION; do
    case $OPTION in
    p)
    PARTITIONS_PER_NODE="$OPTARG"
    ;;
    esac
  done
  shift $((OPTIND-1));
  
  prop_file=$VOLDEMORT_HOME/config/server.properties
  hostname_file=$VOLDEMORT_HOME/hostnames
  
  if [ -e $hostname_file ]; then
     rm -rf $hostname_file
  fi
  touch $hostname_file
  
  # Remove the node id from the existing file
  mv $prop_file $prop_file.bak
  grep -v "^node.id=" $prop_file.bak > $prop_file

  # Set the node id in the server properties file 
  # Also generate the hostname files
  if [[ $# -gt 0 ]]; then
    id=0
    for server in "$@"; do
    if [[ $server == *$PRIVATE_IP* ]]; then
      myid=$id
      echo -e "\nnode.id=$id" >> $prop_file
    fi
    echo -e "$server\n" >> $hostname_file
    id=$((id+1))
    done
  
    if [ -z $myid ]; then
    echo "Could not determine id for my host $PRIVATE_IP against servers $@."
    exit 1
    fi
  else
    echo "Missing server names"
    exit 1
  fi

  # Set up the cluster metadata
  chmod +x $VOLDEMORT_ROOT/contrib/ec2-testing/bin/run-class.sh
  chmod +x $VOLDEMORT_ROOT/contrib/ec2-testing/bin/voldemort-clustergenerator.sh

  $VOLDEMORT_ROOT/contrib/ec2-testing/bin/voldemort-clustergenerator.sh --useinternal true \
    --clustername $CLUSTER_NAME --partitions $PARTITIONS_PER_NODE --hostnames $hostname_file \
    > $VOLDEMORT_HOME/config/cluster.xml
  
}
