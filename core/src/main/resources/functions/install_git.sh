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

function install_git() {
  if ! which git &> /dev/null ; then
    install_git_forced
  fi
}

function install_git_forced() {  
  if which dpkg &> /dev/null; then
    install_git_deb
  elif which yum &> /dev/null; then
    install_git_rpm
  else
    echo "WARNING: could not install git, no appropriate package manager"
  fi
}

function install_git_rpm() {
  if [ `uname -m` == 'x86_64' ]; then
    rpm -Uvh http://download.fedora.redhat.com/pub/epel/5/x86_64/epel-release-5-4.noarch.rpm
  else
    rpm -Uvh http://download.fedora.redhat.com/pub/epel/5/i386/epel-release-5-4.noarch.rpm
  fi
  yum -y install git
}

function install_git_deb() {
  apt-get update -qq
  apt-get -y install git-core
}
