package com.bonree.ants.calc.topology;

import com.bonree.ants.calc.topology.commons.GranuleParams;
import com.bonree.ants.calc.topology.commons.PreprocessParams;
import com.bonree.ants.calc.topology.granule.AggBolt;
import com.bonree.ants.calc.topology.granule.AggSpout;
import com.bonree.ants.calc.topology.preprocess.*;
import com.bonree.ants.calc.topology.schema.BoradcastBolt;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.schema.topology.SchemaSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月10日 上午11:45:57
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 拓扑结构工厂
 * ****************************************************************************
 */
public class TopologyFactory {

    /**
     * 概述：预处理拓扑启动参数集合
     */
    public static final PreprocessParams<Integer> PREPROCESS_PARAMS = new PreprocessParams<>();

    /**
     * 概述：粒度汇总拓扑启动参数集合
     */
    public static final GranuleParams<Integer> GRANULE_PARAMS = new GranuleParams<>();

    /**
     * 概述：初始计算拓扑结构
     *
     * @param topologyName 拓扑名称
     */
    public static StormTopology initTopology(String topologyName) {
        if (GlobalContext.PREPROCESSING.equals(topologyName)) { // 预处理拓扑
            return buildPreprocessTopology();
        } else if (GlobalContext.BIG_GRANULE.equals(topologyName)) { // 大粒度汇总
            int aggInterval = GlobalContext.getConfig().getCalcBigGranuleInterval();
            return buildGranuleTopology(topologyName, aggInterval);
        } else if (GlobalContext.GRANULE.equals(topologyName)) { // 小粒度汇总
            int aggInterval = GlobalContext.getConfig().getCalcGranuleInterval();
            return buildGranuleTopology(topologyName, aggInterval);
        }
        return null;
    }

    /**
     * 概述： 粒度汇总拓扑
     *
     * @param name 用于区分小粒度汇总和大粒度汇总
     */
    private static StormTopology buildGranuleTopology(String name, int aggInterval) {
        TridentTopology topology = new TridentTopology();

        // 广播配置信息
        topology.newStream(name + "_schema", new SchemaSpout())
                .broadcast().name("schemaBolt")
                .each(new Fields("schemaConfs", "schemaBiz"), new BoradcastBolt(), new Fields("info"))
                .parallelismHint(GRANULE_PARAMS.getWorkerNum());

        // 时间粒度汇总业务
        topology.newStream(name, new AggSpout(name, aggInterval))
                .groupBy(new Fields("bizName", "bucketNum")).name("aggBolt")
                .aggregate(new Fields("granule", "bizType", "bizName", "timeList", "bucketNum"), new AggBolt(), new Fields("data"))
                .parallelismHint(GRANULE_PARAMS.getAggNum());
        return topology.build();
    }

    /**
     * 概述：etl处理数据拓扑
     */
    private static StormTopology buildPreprocessTopology() {

        TridentTopology topology = new TridentTopology();

        // 0.广播配置信息
        topology.newStream(GlobalContext.PREPROCESSING + "_schema", new SchemaSpout())
                .broadcast().name("schemaBolt")
                .each(new Fields("schemaConfs", "schemaBiz"), new BoradcastBolt(), new Fields("info"))
                .parallelismHint(PREPROCESS_PARAMS.getWorkerNum());

        // 1.轮循从kafka上拉取数据
        topology.newStream(GlobalContext.PREPROCESSING, new KafkaSpout(KafkaGlobals.SOURCE_TOPIC, PREPROCESS_PARAMS.getBatchNum()))
                .parallelismHint(PREPROCESS_PARAMS.getSpoutThreadNum()).shuffle()

                // 2.将数据均匀分发到各个bolt进行etl处理;
                .name("EtlBolt")// 均匀分散到各个计算节点
                .each(new Fields("records", "time"), new EtlBolt(), new Fields("etime", "dataSet"))
                .parallelismHint(PREPROCESS_PARAMS.getShuffleNum())

                // 3.对etl后的数据进行预处理
                .partitionAggregate(new Fields("dataSet", "etime"), new PartitionAggBolt(), new Fields("bizKey", "dataResult"))

                // 4.全局分组聚合处理
                .groupBy(new Fields("bizKey")).name("groupBolt")
                .aggregate(new Fields("dataResult"), new GroupAggBolt(), new Fields("bizType", "gtime"))
                .parallelismHint(PREPROCESS_PARAMS.getGroupNum())
                .groupBy(new Fields("bizType", "gtime")).name("globalBolt")
                .aggregate(new Fields("bizType", "gtime"), new GlobalBolt(), new Fields("result"));

        return topology.build();
    }
}
