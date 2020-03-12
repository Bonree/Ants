#!/bin/sh

#计算引擎目录
calcpath=$home/jar/Calc_Engine
#存储引擎目录
storagepath=$home/jar/Storage_Engine
#扩展引擎目录
extentionpath=$home/jar/Extention_Engine
#启动日志目录
logdir=$home/logs

#系统时间
TIME=$(date "+%Y-%m-%d %H:%M:%S")

# 启动拓扑
startTopolgy() {
    echo Startup $1 $2! home: $ANTS_HOME, $TIME
    storm jar $SERVER $RUNCLASS $1 $ANTS_HOME $2
    echo Startup $1 completed! $TIME
}

# 停止拓扑, 参数1:拓扑名称, 参数2:数字类型,不为空表示强制停止的延迟时间(秒)
stopTopology() {
    topology=`storm list | grep topology | awk '{print $1}'`
    for var in ${topology[@]};
    do
       if [[ $var == *$1 ]]; then
            echo stop $var [$2] $TIME
            if [ $2 ]; then
               storm kill $var -w $2
            else
               storm kill $var
            fi
            echo stop $var completed! $TIME
       fi
    done
}



