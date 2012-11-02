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
    retry_apt_get -y install krb5-libs krb5-workstation unzip
  elif which rpm &> /dev/null; then
    retry_yum install -y krb5-libs krb5-workstation unzip
  fi
  if [ ! -z "${JDK_INSTALL_URL+xxx}" ]; then
    JCE_POLICY_URL=$(dirname $JDK_INSTALL_URL)"/jce_policy-6.zip"
    wget $JCE_POLICY_URL
    if [ -f jce_policy-6.zip ]; then
	    unzip jce_policy-6.zip
	    mkdir -p /tmp/java_security_old
	    mv /usr/java/default/jre/lib/security/US_export_policy.jar /usr/java/default/jre/lib/security/local_policy.jar /tmp/java_security_old
	    mv jce/*.jar /usr/java/default/jre/lib/security
    fi
  fi
}
