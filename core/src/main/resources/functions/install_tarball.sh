function install_tarball() {
  if [[ "$1" != "" ]]; then
    # Download a .tar.gz file and extract to /usr/local

    local tar_url=$1
    local tar_file=`basename $tar_url`
    local tar_file_md5=`basename $tar_url.md5`

    local curl="curl --silent --show-error --fail --connect-timeout 10 --max-time 600"
    # any download should take less than 10 minutes

    for retry_count in `seq 1 3`;
    do
      $curl -O $tar_url || true
      $curl -O $tar_url.md5 || true

      if md5sum -c $tar_file_md5; then
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

    tar xzf $tar_file -C /usr/local
    rm -f $tar_file $tar_file_md5
  fi
}
