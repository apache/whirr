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
function install_solr() {
  # Install Solr to SOLR_HOME
  local solr_url=$1
  SOLR_HOME=$2
  install_tarball $solr_url $SOLR_HOME

  # Need to move the install files since the tgz drops an extra directory (e.g. apache-solr-4.0-2012-02-04_17-38-34)
  local solr_install_dir=`ls $SOLR_HOME`
  mv $SOLR_HOME/$solr_install_dir/* $SOLR_HOME/
  rm -r $SOLR_HOME/$solr_install_dir

  # Add to ENV
  echo "export SOLR_HOME=$SOLR_HOME" >> /etc/profile
  echo "Installed Solr at $SOLR_HOME"
  exit 0
}
