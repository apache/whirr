function installRunUrl {
  if [ ! -a /usr/bin/runurl ]; then
    wget -qO/usr/bin/runurl run.alestic.com/runurl
    chmod 755 /usr/bin/runurl
  fi
}
