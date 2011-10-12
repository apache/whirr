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

function download() {
  local tar_url=$1
  local tar_file=`basename $tar_url`
  local dest_dir=$2

  for i in `seq 1 3`; do
    curl -L --retry 3 --show-error --fail -O $tar_url
    if tar zxf $tar_file -C $dest_dir ; then
      break;
    else
      rm -f $tar_file
    fi
  done

  if [ ! -e $tar_file ]; then
    echo "Failed to download $tar_url. Aborting."
    exit 1
  fi
}

# -f <configuration URL> -u <voldemort_tar_url>
function install_voldemort() {
  VOLDEMORT_VERSION="0.90"
  VOLDEMORT_TAR_URL=https://github.com/downloads/voldemort/voldemort/voldemort-${VOLDEMORT_VERSION}.tar.gz
  VOLDEMORT_CONF_URL=
  VOLDEMORT_ROOT=/usr/local/voldemort
  VOLDEMORT_HOME=/etc/voldemort
    
  while getopts "f:u:" OPTION; do
    case $OPTION in
    f)
      VOLDEMORT_CONF_URL="$OPTARG"
      ;;
    u)
      VOLDEMORT_TAR_URL="$OPTARG"
      ;;
    esac
  done
    
  # Download the binary distribution and put it in the correct directory.
  download $VOLDEMORT_TAR_URL `dirname $VOLDEMORT_ROOT`
  mv ${VOLDEMORT_ROOT}* $VOLDEMORT_ROOT
    
  # Create our config directory which Voldemort expects to live under $VOLDEMORT_HOME.
  mkdir -p $VOLDEMORT_HOME/config
    
  if [ "$VOLDEMORT_CONF_URL" = "" ] ; then
    # Copy sample property files over to config dir if the user didn't provide a config URL.
    cp $VOLDEMORT_ROOT/config/single_node_cluster/config/* $VOLDEMORT_HOME/config/.
  else
    # Otherwise, download the configuration file contents to the configuration directory.
    download $VOLDEMORT_CONF_URL $VOLDEMORT_HOME/config
  fi
    
  # Set Voldemort vars
  echo "export VOLDEMORT_ROOT=$VOLDEMORT_ROOT" >> /etc/profile
  echo "export VOLDEMORT_HOME=$VOLDEMORT_HOME" >> /etc/profile

  cat >/etc/init.d/voldemort <<END_OF_FILE
#!/bin/sh
# description: Voldemort distributed database server

. /etc/profile

prog="voldemort-server.sh"

start() {
      echo "Starting Voldemort"
      \${VOLDEMORT_ROOT}/bin/voldemort-server.sh >/var/log/voldemort.log 2>&1 &
}

stop() {
      echo "Stopping Voldemort"
      \${VOLDEMORT_ROOT}/bin/voldemort-stop.sh
}

restart(){
      stop
      start
}

case "\$1" in
    start)
          start
          ;;
    stop)
          stop
          ;;
    restart)
          restart
          ;;
    *)
          echo "Usage: voldemort {start|stop|restart}"
          exit 1
esac

exit 0
END_OF_FILE

    chmod +x /etc/init.d/voldemort
    install_service voldemort

}
