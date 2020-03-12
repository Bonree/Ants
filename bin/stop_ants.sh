#!/bin/sh

home=$(cd `dirname $0`; cd ../; pwd)

source $home/bin/common.sh

case $1 in
    ### $2 表示停止拓扑等待的时间，单位秒
    ###停止计算拓扑###
    preprocess|granule|bigGranule)
        cd $calcpath
        sh calc_engine.sh stop $home $1 $2
    ;;
    ###停止入库拓扑###
    storage)
        cd $storagepath
        sh storage_engine.sh stop $home $2
    ;;
    ###停止计算拓扑和存储拓扑###
    all)
        cd $calcpath
        sh calc_engine.sh stop $home $1 $2
        sleep 5
        cd $storagepath
        sh storage_engine.sh stop $home $2
    ;;
    ###停止基线拓扑或报警拓扑###
    baseline|alert)
        cd $extentionpath
        sh extention_engine.sh stop $home $1 $2
    ;;
    ###停止###
    *)
        echo 'args {preprocess | granule | bigGranule | storage | all | baseline | alert}';
    ;;
esac

exit 0