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

function install_ruby() {
  if ! which ruby &> /dev/null; then
    if which dpkg &> /dev/null; then
      install_ruby_deb
      add_to_path
    elif which yum &> /dev/null; then
      install_ruby_rpm
      add_to_path
    else
      echo "ERROR: could not install ruby, no appropriate package manager" 
      return 1
    fi
  fi
}

function install_ruby_rpm() {
  if [ `uname -m` == 'x86_64' ]; then
    rpm -Uvh http://download.fedora.redhat.com/pub/epel/5/x86_64/epel-release-5-4.noarch.rpm
  else
    rpm -Uvh http://download.fedora.redhat.com/pub/epel/5/i386/epel-release-5-4.noarch.rpm
  fi
  yum -y install ruby ruby-rdoc rubygems
}

function install_ruby_deb() {
  apt-get install -y ruby ruby-dev rubygems libopenssl-ruby rdoc ri irb build-essential wget ssl-cert
}

function add_to_path() {
  # add the gems executable directory to the path
  echo "export PATH=$PATH:"`gem environment | grep "EXECUTABLE DIRECTORY" | sed 's|- EXECUTABLE DIRECTORY: ||' | sed -e 's/^[ \t]*//'`"" >> /etc/profile
  echo "export PATH=$PATH:"`gem environment | grep "EXECUTABLE DIRECTORY" | sed 's|- EXECUTABLE DIRECTORY: ||' | sed -e 's/^[ \t]*//'`"" >> /etc/bash.bashrc
}
