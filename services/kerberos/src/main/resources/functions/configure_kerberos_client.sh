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

function configure_kerberos_client() {
  local OPTIND
  local OPTARG
  KERBEROS_SERVER_HOST=localhost
  while getopts "h:" OPTION; do
    case $OPTION in
    h)
      KERBEROS_SERVER_HOST="$OPTARG"
      ;;
    esac
  done
  KERBEROS_REALM_REGEX=$(echo $KERBEROS_REALM | sed s/\\\./\\\\\./g)
  sed -i -e "s/kerberos\.example\.com/$KERBEROS_SERVER_HOST/" /etc/krb5.conf
  sed -i -e "s/example\.com/$KERBEROS_SERVER_HOST/" /etc/krb5.conf
  sed -i -e "s/EXAMPLE\.COM/$KERBEROS_REALM_REGEX/" /etc/krb5.conf
}
