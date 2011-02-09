function update_repo() {
  if which dpkg &> /dev/null; then
    sudo apt-get update
  elif which rpm &> /dev/null; then
    yum update -y yum
  fi
}

function install_hadoop() {
  local OPTIND
  local OPTARG
  
  CLOUD_PROVIDER=
  while getopts "c:" OPTION; do
    case $OPTION in
    c)
      CLOUD_PROVIDER="$OPTARG"
      ;;
    esac
  done

  HADOOP_VERSION=${HADOOP_VERSION:-0.20.2}
  HADOOP_HOME=/usr/local/hadoop-$HADOOP_VERSION

  update_repo

  if ! id hadoop &> /dev/null; then
    useradd hadoop
  fi

  hadoop_tar_url=http://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
  hadoop_tar_file=`basename $hadoop_tar_url`
  hadoop_tar_md5_file=`basename $hadoop_tar_url.md5`

  curl="curl --retry 3 --silent --show-error --fail"
  for i in `seq 1 3`;
  do
    $curl -O $hadoop_tar_url
    $curl -O $hadoop_tar_url.md5
    if md5sum -c $hadoop_tar_md5_file; then
      break;
    else
      rm -f $hadoop_tar_file $hadoop_tar_md5_file
    fi
  done

  if [ ! -e $hadoop_tar_file ]; then
    echo "Failed to download $hadoop_tar_url. Aborting."
    exit 1
  fi

  tar zxf $hadoop_tar_file -C /usr/local
  rm -f $hadoop_tar_file $hadoop_tar_md5_file

  echo "export HADOOP_HOME=$HADOOP_HOME" >> ~root/.bashrc
  echo 'export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH' >> ~root/.bashrc
}

