#!/bin/sh

#程序jar包名称
SERVER=Calc_Engine.jar 

#主启动类路径
RUNCLASS=com.bonree.ants.calc.CalcServer

#预计算拓扑名称
SOURECE_TOPOLOGY=preprocessing_topology

#小粒度汇总拓朴名称
GRANULE_TOPOLOGY=small_granule_topology

#大粒度数据拓朴名称
BIG_GRANULE_TOPOLOGY=big_granule_topology

#上传插件和schema的名称
UPLOAD=upload_schema

#程序主目录
ANTS_HOME=$2

source $ANTS_HOME/bin/common.sh

#原始计算拓扑(case=preprocess)
#小粒度汇总拓扑(case=granule)
#大粒度数据拓扑(case=bigGranule)

case $1 in
    stop)
        case $3 in
            ### $2 表示停止拓扑等待的时间，单位秒
            preprocess)
                stopTopology $SOURECE_TOPOLOGY $4
            ;;
            granule)
                stopTopology $GRANULE_TOPOLOGY $4
            ;;
            bigGranule)
                stopTopology $BIG_GRANULE_TOPOLOGY $4
            ;;
            *)
                sh $0 stop $ANTS_HOME preprocess $4
                sleep 2
                sh $0 stop $ANTS_HOME granule $4
                sleep 2
                sh $0 stop $ANTS_HOME bigGranule $4
            ;;
        esac
    ;;
    preprocess)
        startTopolgy $SOURECE_TOPOLOGY
    ;;
    granule)
        startTopolgy $GRANULE_TOPOLOGY
    ;;
    bigGranule)
        startTopolgy $BIG_GRANULE_TOPOLOGY
    ;;
    upload)
        echo upload schema ......!
        storm jar $SERVER $RUNCLASS $UPLOAD $ANTS_HOME
        echo upload schema completed!
    ;;
    *)
        sh $0 preprocess $ANTS_HOME
        sleep 5
        sh $0 granule $ANTS_HOME
        sleep 5
        sh $0 bigGranule $ANTS_HOME
    ;;
esac

exit 0
