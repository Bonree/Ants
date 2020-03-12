#!/bin/sh

#原始数据etl拓扑(case=preprocess)
#小粒度汇总拓扑(case=granule)
#大粒度数据拓扑(case=bigGranule)
#数据存储拓扑(case=storage)
#同时启动上面四个拓扑(case=all))
#基线汇总拓扑(case=baseline))
#报警汇总拓扑(case=alert)
#上传schema到zk(case=upload)

home=$(cd `dirname $0`; cd ../; pwd)

source $home/bin/common.sh

case $1 in
    ###启动计算拓扑###
    preprocess|granule|bigGranule)
        cd $calcpath
        echo startup $1...
        sh calc_engine.sh $1 $home >> $logdir/$1.out
        echo start $1 complete !!!
    ;;
    ###启动入库拓扑###
    storage)
        cd $storagepath
        echo startup $1...
        # $2 可选参数,为该存储拓扑订阅数据的topic,订阅指标数据时可不填此参数，订阅自定义数据为必须参数
        sh storage_engine.sh $1 $home $2 >> $logdir/$1.out
        echo start $1 complete !!!
    ;;
    ###启动所有拓扑###
    all)
        sh $0 preprocess
        sleep 2
        sh $0 granule
        sleep 2
        sh $0 bigGranule
        sleep 2
        sh $0 storage
    ;;
    ###上传schema到zk###
    upload)
        cd $calcpath
        sh calc_engine.sh $1 $home
    ;;
    ###启动基线拓扑或报警拓扑###
    baseline|alert)
        cd $extentionpath
        echo startup $1...
        sh extention_engine.sh $1 $home >> $logdir/$1.out
        echo start $1 complete !!!
    ;;
    *)
        echo 'args: {preprocess | granule | bigGranule | storage [topology_name]| all | upload | baseline | alert}';
    ;;
esac

exit 0