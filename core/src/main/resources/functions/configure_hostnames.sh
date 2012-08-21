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
function configure_hostnames() {
  local OPTIND
  local OPTARG

  if [ ! -z $AUTO_HOSTNAME_SUFFIX ]; then
      PUBLIC_IP=`/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
      HOSTNAME=${AUTO_HOSTNAME_PREFIX}`echo $PUBLIC_IP | tr . -`${AUTO_HOSTNAME_SUFFIX}
      if [ -f /etc/hostname ]; then
          echo $HOSTNAME > /etc/hostname
      fi
      if [ -f /etc/sysconfig/network ]; then
          sed -i -e "s/HOSTNAME=.*/HOSTNAME=$HOSTNAME/" /etc/sysconfig/network
      fi
      sed -i -e "s/$PUBLIC_IP.*/$PUBLIC_IP $HOSTNAME/" /etc/hosts
      set +e
      if [ -f /etc/init.d/hostname ]; then
          /etc/init.d/hostname restart
      else
          hostname $HOSTNAME
      fi
      set -e
      sleep 2
      hostname
  fi
}
