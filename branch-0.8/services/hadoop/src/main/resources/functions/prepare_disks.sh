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

# This function ensures that all the mount directories in the mapping string
# whose devices are already mounted are available to be used. Symlinks are
# created as necessary.
#
# E.g. suppose the mapping string were /data0,/dev/sdb;/data1,/dev/sdc
# and /dev/sdb were mounted on /mnt, and /dev/sdc was not mounted (possibly
# not even formatted).
# In this case a symlink would be created from /data0 to /mnt. /data1 would
# be created.
function prepare_disks() {
  for mapping in $(echo "$1" | tr ";" "\n"); do
    # Split on the comma (see "Parameter Expansion" in the bash man page)
    mount=${mapping%,*}
    device=${mapping#*,}
    prep_disk $mount $device
  done
}

function prep_disk() {
  mount=$1
  device=$2
  # is device mounted?
  mount | grep -q $device
  if [ $? == 0 ]; then 
    echo "$device is mounted"
    if [ ! -d $mount ]; then
      echo "Symlinking to $mount"
      ln -s $(grep $device /proc/mounts | awk '{print $2}') $mount
    fi
  fi
}
