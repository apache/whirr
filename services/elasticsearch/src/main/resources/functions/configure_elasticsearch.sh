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
    cd /usr/local/elasticsearch-0.15.2/

    for plugin in $@
    do
        ./bin/plugin install $plugin
    done

    cp /tmp/elasticsearch.yml config/elasticsearch.yml

    # use 80% of memory for jvm heap
    local MAXMEM=$(($(free|awk '/^Mem:/{print $2}') * 8 / 10 / 1024))m

    ./bin/elasticsearch -Xmx$MAXMEM -Xms$MAXMEM -p elasticsearch.pid
}

