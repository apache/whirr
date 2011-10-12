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

function update_repo_ganglia() {
  if which dpkg &> /dev/null; then
    sudo apt-get update
  elif which rpm &> /dev/null; then
    #Registers the EPEL repository (this contains the ganglia rpms) 
    rpm -Uvh http://download.fedora.redhat.com/pub/epel/6/$(uname -i)/epel-release-6-5.noarch.rpm
    yum update -y yum
  fi
}

function install_ganglia() {
  local OPTIND
  local OPTARG
  
  local ROLE=
  while getopts "r:" OPTION; do
    case $OPTION in
    r)
      ROLE="$OPTARG"
      ;;
    esac
  done

  update_repo_ganglia
    
  PACKAGES=()

  # add ganglia-monitor to the list of packages to install.
  if which dpkg &> /dev/null; then
    PACKAGES[${#PACKAGES[@]}]=ganglia-monitor
  elif which rpm &> /dev/null; then
    #TODO: check package name for ganglia-monitor. requires epel repo?
    PACKAGES[${#PACKAGES[@]}]=ganglia-gmond
  fi
  
  # For the metad instance, add gmetad to the list of packages to install
  if [ "$ROLE" = "ganglia-metad" ]; then
    if which dpkg &> /dev/null; then
      PACKAGES[${#PACKAGES[@]}]=gmetad
      PACKAGES[${#PACKAGES[@]}]=ganglia-webfrontend
    elif which rpm &> /dev/null; then
      #TODO: check package name for ganglia-monitor. requires epel repo?
      PACKAGES[${#PACKAGES[@]}]=ganglia-gmetad
      PACKAGES[${#PACKAGES[@]}]=ganglia-web
    fi
  fi;
  
  if which dpkg &> /dev/null; then
    #'noninteractive' -> avoids blue configuration screens during installation
    sudo sh -c "DEBIAN_FRONTEND=noninteractive apt-get -y install ${PACKAGES[*]}"
  elif which rpm &> /dev/null; then
    sudo sh -c "yum install -y ${PACKAGES[*]}"
  fi

}