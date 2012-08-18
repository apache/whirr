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

function configure_mahout_client() {
  local OPTARG

  MAHOUT_TAR_URL=
  while getopts "u:" OPTION; do
    case $OPTION in
    u)
      MAHOUT_TAR_URL="$OPTARG"
      ;;
    esac
  done

  MAHOUT_HOME=/usr/local/$(basename $MAHOUT_TAR_URL .tar.gz)

  install_tarball $MAHOUT_TAR_URL
  ln -s $MAHOUT_HOME /usr/local/mahout

  echo "export MAHOUT_HOME=$MAHOUT_HOME" >> /etc/profile
  echo 'export PATH=$MAHOUT_HOME/bin:$PATH' >> /etc/profile
}