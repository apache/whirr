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
function install_elasticsearch() {

    local ES_URL=${1:-https://github.com/downloads/elasticsearch/elasticsearch/elasticsearch-0.17.6.tar.gz}
    install_tarball $ES_URL

    # get the name of the home folder
    local ES_HOME=$(cd /usr/local/elasticsearch-*; pwd)

    # install the latest service wrapper for elasticsearch
    local ES_WRAPPER=https://github.com/elasticsearch/elasticsearch-servicewrapper/tarball/master
    install_tarball $ES_WRAPPER /tmp/

    # move the service wrapper in place
    mv /tmp/elasticsearch-elasticsearch-servicewrapper-*/service $ES_HOME/bin/

    # create a user for running the service
    cat >> /etc/profile <<EOF
export ES_HOME=$ES_HOME
EOF
    useradd -d $ES_HOME elasticsearch

    # update the wrapper configuration files 
    sed -i "s@#RUN_AS_USER=@RUN_AS_USER=elasticsearch@g" $ES_HOME/bin/service/elasticsearch
    sed -i "s@LOCKDIR=.*@LOCKDIR=\"$ES_HOME/lock\"@" $ES_HOME/bin/service/elasticsearch

    # create required folders and update permisions
    mkdir $ES_HOME/lock
    chown -R elasticsearch $ES_HOME

    # increase the max number of open files for this user
    cat >> /etc/security/limits.conf <<EOF
elasticsearch soft nofile 32000
elasticsearch hard nofile 32000
EOF

    # ensure that elasticsearch will start after reboot
    cd $ES_HOME
    ./bin/service/elasticsearch install
}
