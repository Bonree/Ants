package com.bonree.ants.commons.schema.topology;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.zookeeper.NodeListener;
import com.bonree.ants.commons.zookeeper.ZKClient;
import com.bonree.ants.commons.zookeeper.ZKCommons;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年5月9日 下午7:08:14
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 监听Schema配置信息
 * ****************************************************************************
 */
public class SchemaSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(SchemaSpout.class);
    private static final long interval = 60 * 1000;// 用于控制频率

    private SpoutOutputCollector collector; // SPOUT发射器.
    private NodeListener schemaNode;        // schema信息监听
    private volatile long startTime = 0;    // 用于控制频率


    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        // 1.初始配置参数
        Commons.initAntsSchema(conf);

        // 2.监听schema配置信息
        ZKClient client = new ZKClient(GlobalContext.getConfig().getZookeeperCluster());
        schemaNode = new NodeListener(ZKGlobals.SCHEMA_CONFIG_PATH, client.getCuratorFramework());
        schemaNode.addNodeListener();
    }

    @Override
    public void nextTuple() {
        try {
            long currTime = System.currentTimeMillis();
            if ((currTime - startTime) < interval) {
                return; // 1.访问频率小于指定间隔时跳出
            }
            startTime = currTime;
            if (schemaNode != null) {
                String schemaConfig = schemaNode.getData();
                if (schemaConfig != null && !"".equals(schemaConfig)) {
                    byte[] schemaBytes = ZKCommons.instance.getData(ZKGlobals.SCHEMA_CALC_CONFIG_PATH);
                    Schema schema = JSON.parseObject(new String(schemaBytes, StandardCharsets.UTF_8), Schema.class);
                    collector.emit(new Values(schemaConfig, schema));
                    schemaNode.clear();
                    log.info("Monitoring schema config info: {}", schemaConfig);
                    log.info("Monitoring schema biz info: {}", JSON.toJSONString(schema.getData().keySet()));
                }
            }
        } catch (Exception ex) {
            log.error("SchemaSpout error!", ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("schemaConfs", "schemaBiz"));
    }
}
