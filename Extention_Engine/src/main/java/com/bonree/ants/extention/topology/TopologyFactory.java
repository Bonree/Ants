package com.bonree.ants.extention.topology;

import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.extention.topology.alert.AggAlertBolt;
import com.bonree.ants.extention.topology.alert.AggAlertSpout;
import com.bonree.ants.extention.topology.baseline.AggBaselineBolt;
import com.bonree.ants.extention.topology.baseline.AggBaselineSpout;
import com.bonree.ants.extention.topology.commons.ExtentionParams;
import com.bonree.ants.extention.topology.schema.BroadcastBolt;
import com.bonree.ants.commons.schema.topology.SchemaSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class TopologyFactory {

    /**
     * 概述：扩展拓扑启动参数
     */
    public static final ExtentionParams<Integer> EXTENTION_PARAMS = new ExtentionParams<>();

    /**
     * 概述：初始拓扑结构
     *
     * @param topologyName 拓扑名称
     */
    public static StormTopology initTopology(String topologyName) {
        if (GlobalContext.BASELINE.equals(topologyName)) {
            return buildBaselineTopology(topologyName); // 基线拓扑
        } else {
            return buildAlertTopology(topologyName); // 报警拓扑
        }
    }

    /**
     * 概述：初始基线拓扑结构
     *
     * @param topologyName 拓扑名称
     */
    private static StormTopology buildBaselineTopology(String topologyName) {
        TridentTopology topology = new TridentTopology();

        // 0.广播配置信息
        topology.newStream(topologyName + "_schema", new SchemaSpout())
                .broadcast().name("schemaBolt").
                each(new Fields("schemaConfs", "schemaBiz"), new BroadcastBolt(), new Fields("info"))
                .parallelismHint(EXTENTION_PARAMS.getWorkerNum());

        // 1.时间粒度汇总业务
        topology.newStream(topologyName, new AggBaselineSpout(topologyName, GlobalContext.getConfig().getCalcBaselineInterval()))
                .groupBy(new Fields("bizName", "bucketNum")).name("aggBolt")
                .aggregate(new Fields("granule", "bizType", "bizName", "timeList", "bucketNum"), new AggBaselineBolt(), new Fields("data"))
                .parallelismHint(EXTENTION_PARAMS.getAggNum());
        return topology.build();
    }

    /**
     * 概述：初始报警拓扑结构
     *
     * @param topologyName 拓扑名称
     */
    private static StormTopology buildAlertTopology(String topologyName) {
        TridentTopology topology = new TridentTopology();

        // 0.广播配置信息
        topology.newStream(topologyName + "_schema", new SchemaSpout())
                .broadcast().name("schemaBolt").
                each(new Fields("schemaConfs", "schemaBiz"), new BroadcastBolt(), new Fields("info"))
                .parallelismHint(EXTENTION_PARAMS.getWorkerNum());

        // 1.时间粒度汇总业务
        topology.newStream(topologyName, new AggAlertSpout(topologyName, GlobalContext.getConfig().getCalcAlertInterval()))
                .groupBy(new Fields("bizName", "bucketNum")).name("aggBolt")
                .aggregate(new Fields("time", "cycleList", "bizType", "bizName", "bucketNum"), new AggAlertBolt(KafkaGlobals.ALERT_TOPIC), new Fields("data"))
                .parallelismHint(EXTENTION_PARAMS.getAggNum());
        return topology.build();
    }
}
