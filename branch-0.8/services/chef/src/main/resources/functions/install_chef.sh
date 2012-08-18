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

function install_chef() {
	
	# this script assumes ruby and ruby gems are already installed
	
	# install the chef solo required gems
	sudo gem install chef --no-rdoc --no-ri --bindir /usr/bin/
	sudo gem install ohai --no-rdoc --no-ri --bindir /usr/bin/
		
	# dowload and unpack the cookbooks
	
		
	# create the solo.rb chef config file
	mkdir /etc/chef
	echo 'file_cache_path "/tmp/chef-solo"
cookbook_path "/tmp/chef-solo/cookbooks"' >> /etc/chef/solo.rb

	# create the cache dir
	mkdir /tmp/chef-solo

	# workaround for a problem where cookbooks are dowloaded directly 
	# to the cache dir instead of the cache/cookbook dir
	ln -s /tmp/chef-solo/ /tmp/chef-solo/cookbooks
	
	chmod -R 777 /tmp/chef-solo
	
	# download all the 'official' cookbooks using git
	# NOTE: this will effectively overwrite any previously installed cookbooks with the same name
	cd /tmp/chef-solo/cookbooks
	mkdir temp
	cd temp
	
	git clone https://github.com/opscode/cookbooks.git .
	
	# reset the repo to the last commit we know works. probably we could pass this value
    git reset --hard d101a72aa39eb217bfe7
	
	cd ..
	mv -f temp/* .
	
	# cleanup
	rm -rf temp
	
	# these cookbooks cause problems (at least in installing postgresql)
	rm -rf openldap
	rm -rf windows
	rm -rf iis
	rm -rf webpi
}