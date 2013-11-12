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

set -x

function configure_kerberos_server() {
  KERBEROS_USER=${KERBEROS_USER:-$CLUSTER_USER}
  KERBEROS_REALM_REGEX=$(echo $KERBEROS_REALM | sed s/\\\./\\\\\./g)
  if which dpkg &> /dev/null; then
    KERBEROS_HOME=/etc/krb5kdc
    KERBEROS_SERVICE_KDC=krb5-kdc
    KERBEROS_SERVICE_ADMIN=krb5-admin-server
    export DEBIAN_FRONTEND=noninteractive
    retry_apt_get update
    retry_apt_get -q -y install expect
  elif which rpm &> /dev/null; then
    KERBEROS_HOME=/var/kerberos/krb5kdc
    KERBEROS_SERVICE_KDC=krb5kdc
    KERBEROS_SERVICE_ADMIN=kadmin
    retry_yum install -y expect
  fi
  service $KERBEROS_SERVICE_KDC stop
  service $KERBEROS_SERVICE_ADMIN stop
  sed -i -e "s/EXAMPLE\.COM/$KERBEROS_REALM_REGEX/" $KERBEROS_HOME/kdc.conf
  cat >> run_kdb5_util <<END
#!/usr/bin/expect -f
set timeout 5000
spawn sudo kdb5_util create -s
expect {Enter KDC database master key: } { send "admin\r" }
expect {Re-enter KDC database master key to verify: } { send "admin\r" }
expect EOF
END
  chmod +x run_kdb5_util
  ./run_kdb5_util
  rm -rf run_kdb5_util
  if [ -f $KERBEROS_HOME/kadm5.acl ]; then
    sed -i -e "s/EXAMPLE\.COM/$KERBEROS_REALM_REGEX/" $KERBEROS_HOME/kadm5.acl
  else
    echo "*/admin@$KERBEROS_REALM	*" > $KERBEROS_HOME/kadm5.acl
  fi
  cat >> run_addpinc <<END
#!/usr/bin/expect -f
set timeout 5000
set principal_primary [lindex \$argv 0]
set principal_instance [lindex \$argv 1]
set realm [lindex \$argv 2]
spawn sudo kadmin.local -q "addprinc \$principal_instance@\$realm"
expect -re {Enter password for principal .*} { send "\$principal_primary\r" }
expect -re {Re-enter password for principal .* } { send "\$principal_primary\r" }
expect EOF
END
  chmod +x run_addpinc
  ./run_addpinc $KERBEROS_USER $KERBEROS_USER/admin $KERBEROS_REALM
  ./run_addpinc $KERBEROS_USER $KERBEROS_USER $KERBEROS_REALM
  ./run_addpinc hdfs hdfs $KERBEROS_REALM
  rm -rf ./run_addpinc
  service $KERBEROS_SERVICE_KDC start
  service $KERBEROS_SERVICE_ADMIN start
  CONFIGURE_KERBEROS_DONE=1
}
