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
function install_tarball() {
  if [[ "$1" != "" ]]; then
    # Download a .tar.gz file and extract to target dir

    local tar_url=$1
    local tar_file=`basename $tar_url`
    local tar_file_md5=`basename $tar_url.md5`

    local target=${2:-/usr/local/}
    mkdir -p $target

    local curl="curl -L --silent --show-error --fail --connect-timeout 10 --max-time 600 --retry 5"
    # any download should take less than 10 minutes

    for retry_count in `seq 1 3`;
    do
      $curl -O $tar_url || true
      $curl -O $tar_url.md5 || true

      if [ ! -e $tar_file_md5 ]; then
        echo "Could not download  $tar_url.md5. Continuing."
        break;
      elif md5sum -c $tar_file_md5; then
        break;
      else
        # workaround for cassandra broken .md5 files
        if [ `md5sum $tar_file | awk '{print $1}'` = `cat $tar_file_md5` ]; then
          break;
        fi

        rm -f $tar_file $tar_file_md5
      fi

      if [ ! $retry_count -eq "3" ]; then
        sleep 10
      fi
    done

    if [ ! -e $tar_file ]; then
      echo "Failed to download $tar_file. Aborting."
      exit 1
    fi

    tar xzf $tar_file -C $target
    rm -f $tar_file $tar_file_md5
  fi
}
