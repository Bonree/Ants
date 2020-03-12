#!/bin/sh

#程序jar包名称
SERVER=Storage_Engine.jar 

#主启动类路径
RUNCLASS=com.bonree.ants.storage.StorageServer

#存储拓扑名称
STORAGE_TOPOLOGY=storage_topology

#程序主目录
ANTS_HOME=$2

source $ANTS_HOME/bin/common.sh

case $1 in
    stop)
        # $3 表示停止拓扑等待的时间，单位秒
        stopTopology $STORAGE_TOPOLOGY $3
    ;;
    insert)
        # $3 可选参数,为该存储拓扑订阅数据的topic,订阅指标数据时可不填此参数，订阅自定义数据为必须参数
        startTopolgy $STORAGE_TOPOLOGY $3
    ;;
    *)
        sh $0 insert $ANTS_HOME $3
    ;;
esac

exit 0