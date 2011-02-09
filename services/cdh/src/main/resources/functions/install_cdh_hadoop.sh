function update_repo() {
  if which dpkg &> /dev/null; then
    cat > /etc/apt/sources.list.d/cloudera.list <<EOF
deb http://archive.cloudera.com/debian lucid-$REPO contrib
deb-src http://archive.cloudera.com/debian lucid-$REPO contrib
EOF
    curl -s http://archive.cloudera.com/debian/archive.key | sudo apt-key add -
    sudo apt-get update
  elif which rpm &> /dev/null; then
    rm -f /etc/yum.repos.d/cloudera.repo
    REPO_NUMBER=`echo $REPO | sed -e 's/cdh\([0-9][0-9]*\)/\1/'`
    cat > /etc/yum.repos.d/cloudera-$REPO.repo <<EOF
[cloudera-$REPO]
name=Cloudera's Distribution for Hadoop, Version $REPO_NUMBER
mirrorlist=http://archive.cloudera.com/redhat/cdh/$REPO_NUMBER/mirrors
gpgkey = http://archive.cloudera.com/redhat/cdh/RPM-GPG-KEY-cloudera
gpgcheck = 0
EOF
    yum update -y yum
  fi
}

function install_cdh_hadoop() {
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
  
  REPO=${REPO:-cdh3}
  HADOOP=hadoop-${HADOOP_VERSION:-0.20}
  HADOOP_CONF_DIR=/etc/$HADOOP/conf.dist

  update_repo
  
  if which dpkg &> /dev/null; then
    apt-get update
    apt-get -y install $HADOOP
    cp -r /etc/$HADOOP/conf.empty $HADOOP_CONF_DIR
    update-alternatives --install /etc/$HADOOP/conf $HADOOP-conf $HADOOP_CONF_DIR 90
  elif which rpm &> /dev/null; then
    yum install -y $HADOOP
    cp -r /etc/$HADOOP/conf.empty $HADOOP_CONF_DIR
    alternatives --install /etc/$HADOOP/conf $HADOOP-conf $HADOOP_CONF_DIR 90
  fi
}
