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
# are available to be used. This is achieved by formatting, mounting, and 
# symlinking (if the volume is already mounted as another directory).
#
# E.g. suppose the mapping string were /data0,/dev/sdb;/data1,/dev/sdc
# and /dev/sdb were mounted on /mnt, and /dev/sdc was not mounted or formatted.
# In this case a symlink would be created from /data0 to /mnt. /dev/sdc would
# be formatted, then mounted on /data1.
function prepare_all_disks() {
  for mapping in $(echo "$1" | tr ";" "\n"); do
    # Split on the comma (see "Parameter Expansion" in the bash man page)
    mount=${mapping%,*}
    device=${mapping#*,}
    prep_disk $mount $device
  done
  # Make sure there's at least a /data0 and /data (on the root filesystem)
  if [ ! -e /data0 ]; then
    if [ -e /data ]; then
      ln -s /data /data0
    else
      mkdir /data0
      ln -s /data0 /data
    fi
  else
    if [ ! -e /data ]; then
      ln -s /data0 /data
    fi
  fi
}

function prep_disk() {
  mount=$1
  device=$2
  automount=${3:-false}

  if [ ! -e $device ]; then
      device=$(echo "$device"|sed -e 's/\/sd/\/xvd/')
  fi

  # match /dev/sd* devices to their Xen VPS equivalents and set device if found
  deviceXen=$(echo "$device"|sed -e 's/\/sd/\/xvd/')
  if [ ! -e $deviceXen ]; then
    # match /dev/sd(.) to a new RHEL 6.1 Xen VPS naming scheme - https://bugzilla.redhat.com/show_bug.cgi?id=729586
    deviceXen=$(echo "$device"|sed -e 's/\/sd./\/xvd/'|xargs -I £ echo "£"$(printf \\$(printf '%03o' $(($(printf "%d\n" \'${device:${#device} - 1})+4)) )))
  fi
  if [ -e $deviceXen ]; then
    device=$deviceXen
  fi

  if [ -e $device ]; then
    # is device formatted?
    if [ $(mountpoint -q -x $device) ]; then
      echo "$device is formatted"
    else
      if which dpkg &> /dev/null; then
        apt-get install -y xfsprogs
      elif which rpm &> /dev/null; then
        yum install -y xfsprogs
      fi
      echo "warning: ERASING CONTENTS OF $device"
      mkfs.xfs -f $device
    fi
    # is device mounted?
    mount | grep -q $device
    if [ $? == 0 ]; then 
      echo "$device is mounted"
      if [ ! -d $mount ]; then
        echo "Symlinking to $mount"
        ln -s $(grep $device /proc/mounts | awk '{print $2}') $mount
      fi
    else
      echo "Mounting $device on $mount"
      if [ ! -e $mount ]; then
        mkdir $mount
      fi
      mount -o defaults,noatime $device $mount
      if $automount ; then
        echo "$device $mount xfs defaults,noatime 0 0" >> /etc/fstab
      fi
    fi
  fi
}
