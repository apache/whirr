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

function install_puppet() {
  if ! which puppet &> /dev/null ; then
    install_puppet_forced
  fi
}

function install_puppet_forced() {
  # this script assumes ruby and ruby gems are already installed
 
  # Setup the default module/fact path locations so we can populate them
  # elsewhere
  mkdir -p /etc/puppet/modules
  mkdir -p /etc/puppet/manifests
  mkdir -p /usr/share/puppet/modules
  mkdir -p /var/lib/puppet/lib/facter

  # install the puppet and facter gems
  sudo gem install facter --no-rdoc --no-ri --bindir /usr/bin/
  sudo gem install puppet --no-rdoc --no-ri --bindir /usr/bin/
 
  sudo useradd puppet
}
