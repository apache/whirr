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

function install_kerberos_client() {
  if which dpkg &> /dev/null; then
    export DEBIAN_FRONTEND=noninteractive
    retry_apt_get update
    retry_apt_get -q -y install krb5-user krb5-config unzip
  elif which rpm &> /dev/null; then
    retry_yum install -y krb5-libs krb5-workstation unzip
  fi
  if [ -z "${JAVA_HOME+xxx}" ]; then
    if which java &> /dev/null; then
      JAVA_HOME=$(readlink -f $(which java) | sed "s:/bin/java::")
    fi
  fi
  if [ ! -z "${JAVA_HOME+xxx}" ]; then
    JAVA_VERSION_MAJOR=$($JAVA_HOME/bin/java -version 2>&1 | grep "java version" | sed 's/java version \"1\.\([0-9]*\)\..*/\1/')
    if [ "$JAVA_VERSION_MAJOR" == "6" ]; then
        JAVA_JCE=jce_policy-6.zip
    elif [ "$JAVA_VERSION_MAJOR" == "7" ]; then
        JAVA_JCE=UnlimitedJCEPolicyJDK7.zip
    fi
    if [ ! -z "${JAVA_JCE+xxx}" ]; then
      if [ ! -z "${JDK_INSTALL_URL+xxx}" ]; then
        wget -nv $(dirname $JDK_INSTALL_URL)"/"$JAVA_JCE
        if [ -f $JAVA_JCE ]; then
        unzip -q -o -j $JAVA_JCE -d $JAVA_HOME/jre/lib/security
          rm $JAVA_JCE
        fi
      fi
    fi
  fi
}
