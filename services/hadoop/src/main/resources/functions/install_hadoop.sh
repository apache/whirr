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
  
  install_tarball http://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

  echo "export HADOOP_HOME=$HADOOP_HOME" >> ~root/.bashrc
  echo 'export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH' >> ~root/.bashrc
}

