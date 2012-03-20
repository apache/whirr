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
function install_oracle_jdk6() {
  target_dir=/usr/lib/jvm/java-6-sun

  if [ -e "$target_dir" ]; then
    echo "It appears java is already installed. Skipping java installation."
    echo "Move /usr/lib/jvm/java-6-sun out of the way if you want to reinstall"

    #nasty little hack... somewhere the string 'r e t u r n' gets replaced by exit
    turn=turn
    re$turn
  fi

  arch=`uname -m`

  # Find out which .bin file to download
  url=http://download.oracle.com/otn-pub/java/jdk/6u31-b04/jdk-6u31-linux-i586.bin
  if [ "x86_64" == "$arch" ]; then
    url=http://download.oracle.com/otn-pub/java/jdk/6u31-b04/jdk-6u31-linux-x64.bin
  fi
  
  tmpdir=`mktemp -d`
  curl $url -L --silent --show-error --fail --connect-timeout 60 --max-time 600 --retry 5 -o $tmpdir/`basename $url`

  (cd $tmpdir; sh `basename $url` -noregister)
  mkdir -p `dirname $target_dir`
  (cd $tmpdir; mv jdk1* $target_dir)
  rm -rf $tmpdir
  
  if which dpkg &> /dev/null; then
    update-alternatives --install /usr/bin/java java $target_dir/bin/java 17000
    update-alternatives --set java $target_dir/bin/java
  elif which rpm &> /dev/null; then
    alternatives --install /usr/bin/java java $target_dir/bin/java 17000
    alternatives --set java $target_dir/bin/java
  else
    # Assume there is no alternatives mechanism, create our own symlink
    ln -sf "$target_dir/bin/java" /usr/bin/java
  fi

  # Try to set JAVA_HOME in a number of commonly used locations
  export JAVA_HOME=$target_dir
  if [ -f /etc/profile ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/profile
  fi
  if [ -f /etc/bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/bashrc
  fi
  if [ -f ~root/.bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> ~root/.bashrc
  fi
  if [ -f /etc/skel/.bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/skel/.bashrc
  fi
  if [ -f "$DEFAULT_HOME/$NEW_USER" ]; then
    echo export JAVA_HOME=$JAVA_HOME >> $DEFAULT_HOME/$NEW_USER
  fi
}
