package com.bonree.ants.storage.topology;

import com.bonree.ants.commons.schema.topology.SchemaSpout;
import com.bonree.ants.storage.commons.StorageParams;
import com.bonree.ants.storage.topology.schema.BroadcastBolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.List;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月11日 上午13:25:52
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 拓扑结构工厂
 * ****************************************************************************
 */
public class TopologyFactory {

    public static final StorageParams<Integer> STORAGE_PARAMS = new StorageParams<>(); // 存储拓扑启动参数

    /**
     * 初始存储拓扑结构
     * @param topics 订阅数据的topic集合
     * @param paramTopic 启动拓扑时的topic参数（用来区分存储拓扑订阅数据）
     * @return
     */
    public static StormTopology initTopology(List<String> topics, String paramTopic) {
        TopologyBuilder builder = new TopologyBuilder();
        // 0.广播配置信息
        builder.setSpout("schema_spout", new SchemaSpout());
        builder.setBolt("schema_bolt", new BroadcastBolt(paramTopic), STORAGE_PARAMS.getWorkerNum()).allGrouping("schema_spout");

        // 1.数据存储
        builder.setSpout("insert_spout", new KafkaSpout(topics), STORAGE_PARAMS.getSpoutThreadNum());
        builder.setBolt("insert_bolt", new StorageBolt(), STORAGE_PARAMS.getShuffleNum()).shuffleGrouping("insert_spout");
        return builder.createTopology();
    }
}
