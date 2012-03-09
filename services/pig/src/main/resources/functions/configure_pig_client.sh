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

function configure_pig_client() {

	local OPTARG

	PIG_TAR_URL=
	while getopts "u:" OPTION; do
	case $OPTION in
	u)
	  PIG_TAR_URL="$OPTARG"
	  ;;
	esac
	done

	PIG_HOME=/usr/local/$(basename $PIG_TAR_URL .tar.gz)

	install_tarball $PIG_TAR_URL
	ln -s $PIG_HOME /usr/local/pig 

	if [ -f /etc/profile ]; then
		echo "export PIG_HOME=$PIG_HOME" >> /etc/profile
		echo 'export PATH=$PIG_HOME/bin:$PATH' >> /etc/profile
		echo 'export PIG_CLASSPATH=/etc/hadoop/conf' >> /etc/profile
	fi
	if [ -f /etc/bashrc ]; then
		echo "export PIG_HOME=$PIG_HOME" >> /etc/bashrc
		echo 'export PATH=$PIG_HOME/bin:$PATH' >> /etc/bashrc
		echo 'export PIG_CLASSPATH=/etc/hadoop/conf' >> /etc/bashrc
	fi
	if [ -f /etc/skel/.bashrc ]; then
		echo "export PIG_HOME=$PIG_HOME" >> /etc/skel/.bashrc
		echo 'export PATH=$PIG_HOME/bin:$PATH' >> /etc/skel/.bashrc
		echo 'export PIG_CLASSPATH=/etc/hadoop/conf' >> /etc/skel/.bashrc
	fi
  
  
}

