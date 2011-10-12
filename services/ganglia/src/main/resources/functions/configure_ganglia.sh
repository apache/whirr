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
function configure_ganglia() {
  local OPTIND
  local OPTARG

  GRID_NAME=WhirrGrid  
  METAD_HOST=localhost
  while getopts "m:" OPTION; do
    case $OPTION in
    m)
      METAD_HOST="$OPTARG"
      ;;
    esac
  done

  # The service scripts have different names on different distros
  SVC_APACHE=apache2
  SVC_GMOND=ganglia-monitor
  SVC_GMETAD=gmetad
  
  if which rpm &> /dev/null; then 
      SVC_GMOND=gmond
      SVC_APACHE=httpd
  fi
    
  function remove_ganglia_conf_section() {
    section_name=$1
    file=$2

    sed -i -n '1h;1!H;${;g;s/'$section_name' {[^}]*}//g;p;}' "$2"
  }
  
  # create the conf.d directory included in gmond.conf
  mkdir -p /etc/ganglia/conf.d
    
  # On the master, update gmond and gmetad
  echo "Comparing self with metad_host: $PRIVATE_IP == $METAD_HOST"
  if [ "$PRIVATE_IP" == "$METAD_HOST" ]; then
    
    ### Configure the gmetad instance
    
    remove_ganglia_conf_section cluster /etc/ganglia/gmond.conf
    remove_ganglia_conf_section host /etc/ganglia/gmond.conf
    remove_ganglia_conf_section udp_send_channel /etc/ganglia/gmond.conf
    remove_ganglia_conf_section udp_recv_channel /etc/ganglia/gmond.conf
    remove_ganglia_conf_section tcp_accept_channel /etc/ganglia/gmond.conf
    
    cat > /etc/ganglia/conf.d/cluster.conf <<EOF
cluster {
  name = "$CLUSTER_NAME"
  owner = "unspecified"
  latlong = "unspecified"
  url = "unspecified"
}
EOF

    cat > /etc/ganglia/conf.d/host.conf <<EOF
host {
  location = "unspecified"
}
EOF

    cat > /etc/ganglia/conf.d/channels.conf <<EOF
udp_send_channel {
  host = $METAD_HOST
  port = 8649
  ttl = 1
}

udp_recv_channel {
  bind = $METAD_HOST
  port = 8649
}

tcp_accept_channel {
  port = 8649
}
EOF

    # create the the gmetad.conf file
    cat > /etc/ganglia/gmetad.conf <<EOF
    gridname "$GRID_NAME"
    data_source "$CLUSTER_NAME" $METAD_HOST
EOF

    if which dpkg &> /dev/null; then
      # For debian-based systems we need to copy the apache configuration in place
      cp /etc/ganglia-webfrontend/apache.conf /etc/apache2/sites-enabled
      service $SVC_GMOND restart || true
      service $SVC_GMETAD restart || true
      service $SVC_APACHE restart || true
    elif which rpm &> /dev/null; then
      # For rpm based systems the apache conf file is automatically copied to /etc/apache/conf.d
      service $SVC_GMOND restart || true
      service $SVC_GMETAD restart || true
      service $SVC_APACHE restart || true # not sure if this is needed
    fi
    
  else
    
    ### Configure the monitor instances

    remove_ganglia_conf_section cluster /etc/ganglia/gmond.conf
    remove_ganglia_conf_section host /etc/ganglia/gmond.conf
    remove_ganglia_conf_section udp_send_channel /etc/ganglia/gmond.conf
    remove_ganglia_conf_section udp_recv_channel /etc/ganglia/gmond.conf
    remove_ganglia_conf_section tcp_accept_channel /etc/ganglia/gmond.conf
    
    cat > /etc/ganglia/conf.d/cluster.conf <<EOF
cluster {
  name = "$CLUSTER_NAME"
  owner = "unspecified"
  latlong = "unspecified"
  url = "unspecified"
}
EOF
    
    cat > /etc/ganglia/conf.d/channels.conf <<EOF
udp_send_channel {
  host = $METAD_HOST
  port = 8649
  ttl = 1
}

# This section should be commented out, but must be present because of a bug in gmond
# TODO: find out if that bug is already fixed 
udp_recv_channel {
  bind = localhost
  port = 8649
}

tcp_accept_channel {
  port = 8649  
}
EOF

    service $SVC_GMOND restart

  fi
  
}