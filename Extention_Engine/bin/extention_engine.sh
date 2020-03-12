#!/bin/sh

#程序jar包名称
SERVER=Extention_Engine.jar

#主启动类路径
RUNCLASS=com.bonree.ants.extention.ExtentionServer

#基线拓扑名称
BASELINE_TOPOLOGY=baseline_topology

#报警计算拓扑名称
ALERT_TOPOLOGY=alert_topology

#程序主目录
ANTS_HOME=$2

case $1 in
    stop)
        case $3 in
            alert)
                stopTopology $ALERT_TOPOLOGY $4
            ;;
            baseline)
                stopTopology $BASELINE_TOPOLOGY $4
            ;;
        esac
    ;;
    alert)
        startTopolgy $ALERT_TOPOLOGY
    ;;
    baseline)
        startTopolgy $BASELINE_TOPOLOGY
    ;;
    *)
        echo 'args: {baseline | alert | stop baseline | stop alert}';
    ;;
esac

exit 0
