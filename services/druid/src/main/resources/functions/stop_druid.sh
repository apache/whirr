function stop_druid() {

    ps -eaf | grep RealtimeMain | grep -v grep | awk '{print $2}' | xargs kill
    ps -eaf | grep MasterMain | grep -v grep | awk '{print $2}' | xargs kill
    ps -eaf | grep ComputeMain | grep -v grep | awk '{print $2}' | xargs kill
    ps -eaf | grep BrokerMain | grep -v grep | awk '{print $2}' | xargs kill

}
