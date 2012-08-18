#!/bin/bash
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
# Based on http://blog.lars-francke.de/2010/08/16/performance-testing-hbase-using-ycsb/
#

set -x -e 

sudo rm -rf /tmp/ycsb
sudo mkdir -p /tmp/ycsb
cd /tmp/ycsb

sudo curl -s -O http://people.apache.org/~asavu/ycsb-0.1.3.tar.gz
sudo tar xfz ycsb-0.1.3.tar.gz

cd /usr/local/hbase-*
sudo ./bin/hbase shell <<EOF
disable 'usertable'
drop 'usertable'
create 'usertable', 'family'
exit
EOF

cd /tmp/ycsb/ycsb-0.1.3/

sudo java -cp build/ycsb.jar:db/hbase/lib/* com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.db.HBaseClient -P workloads/workloada -p columnfamily=family -p recordcount=2000 -s > load.dat

sudo java -cp build/ycsb.jar:db/hbase/lib/* com.yahoo.ycsb.Client -t -db com.yahoo.ycsb.db.HBaseClient -P workloads/workloada -p columnfamily=family -p operationcount=2000 -s -threads 10 -target 100 > transactions.dat

echo "*** Data loading stats"
sudo head -n 11 load.dat

echo "*** Transactions stats"
sudo head -n 11 transactions.dat

