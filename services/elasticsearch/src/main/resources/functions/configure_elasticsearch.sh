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
function configure_elasticsearch() {
    . /etc/profile

    cd $ES_HOME
    for plugin in $@
    do
        ./bin/plugin install $plugin
    done

    # Use no more than 70% of the available RAM for heap
    local ES_MIN_MEM=256
    local ES_MAX_MEM=$(($(free|awk '/^Mem:/{print $2}') * 7 / 10 / 1024))

    cat >> /etc/profile <<EOF
export ES_MIN_MEM=256
export ES_MAX_MEM=$ES_MAX_MEM
EOF

    cp /tmp/elasticsearch.yml config/elasticsearch.yml

    chown -R elasticsearch $ES_HOME
}

