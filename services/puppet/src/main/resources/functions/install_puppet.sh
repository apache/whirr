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
    install_puppet_forced $1
  fi
}

function install_puppet_forced() {
  # this script assumes ruby and ruby gems are already installed
 
  # Setup the default module/fact path locations so we can populate them
  # elsewhere
  mkdir -p /etc/puppet/modules
  mkdir -p /etc/puppet/manifests/extdata
  mkdir -p /usr/share/puppet/modules
  mkdir -p /var/lib/puppet/lib/facter

  if [ -z "$1" ] ; then # install the puppet and facter gems
    sudo gem install facter --no-rdoc --no-ri --bindir /usr/bin/
    sudo gem install puppet --no-rdoc --no-ri --bindir /usr/bin/
 
    sudo useradd puppet
  else # install puppet from a given repo
    case $1 in
      *.deb)
         REPOMGMT=apt-get
         TMPDIR=`mktemp -d /tmp/whirr-XXXXXXXXXX`
         PKGNAME=`basename $1`
         yes | apt-get install -y wget
         (cd $TMPDIR ; wget "$1")
         dpkg -i $TMPDIR/$PKGNAME
         rm -f $TMPDIR/$PKGNAME
         rmdir $TMPDIR
         apt-get update
         ;;
      *.rpm)
         rpm -i "$1"
         if which zypper ; then
           REPOMGMT=zypper
           zypper clean
         else
           REPOMGMT=yum
           yum clean all
         fi
         ;;
      *.repo)
         if zypper ar -f "$1" ; then
           REPOMGMT=zypper
         else
           REPOMGMT=yum
           yes | yum install -y wget
           (cd /etc/yum.repos.d && wget "$1")
           yum clean all
         fi
         ;;
      *.list)
         REPOMGMT=apt-get
         (cd /etc/apt/sources.list.d ; wget "$1")
         apt-get update
         ;;
      *)
         REPOMGMT=zypper
         zypper ar -f "$1" `basename "$1"`
         ;;
    esac
    yes | $REPOMGMT install -y puppet
  fi
}
