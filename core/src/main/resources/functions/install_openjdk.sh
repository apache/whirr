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
function install_openjdk_deb() {
  apt-get update
  apt-get -y install openjdk-6-jdk
  
  export JAVA_HOME=/usr/lib/jvm/java-6-openjdk
  echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile
  echo "export JAVA_HOME=$JAVA_HOME" >> ~root/.bashrc
  update-alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 17000
  update-alternatives --set java $JAVA_HOME/bin/java
  java -version
  
}

function install_openjdk_rpm() {
  yum install java-1.6.0-openjdk
  
  export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk
  echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile
  echo "export JAVA_HOME=$JAVA_HOME" >> ~root/.bashrc
  alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 17000
  alternatives --set java $JAVA_HOME/bin/java
  java -version
}

function install_openjdk() {
  if which dpkg &> /dev/null; then
    install_openjdk_deb
  elif which rpm &> /dev/null; then
    install_openjdk_rpm
  fi
}
