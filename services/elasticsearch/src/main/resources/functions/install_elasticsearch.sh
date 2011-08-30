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

    # TODO Run ElasticSearch as non-root-user
    # http://www.elasticsearch.org/tutorials/2011/02/22/running-elasticsearch-as-a-non-root-user.html

    local ES_URL=${1:-https://github.com/downloads/elasticsearch/elasticsearch/elasticsearch-0.17.6.tar.gz}
    install_tarball $ES_URL

    # install the latest service wrapper for elasticsearch
    local ES_WRAPPER=https://github.com/elasticsearch/elasticsearch-servicewrapper/tarball/master
    install_tarball $ES_WRAPPER /tmp/

    # move the service wrapper in place
    mv /tmp/elasticsearch-elasticsearch-servicewrapper-*/service /usr/local/elasticsearch-*/bin/
    cd /usr/local/elasticsearch-*

    # ensure that elasticsearch will start after reboot
    ./bin/service/elasticsearch install
}
