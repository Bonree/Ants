package com.bonree.ants.extention;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.engine.EngineServer;
import com.bonree.ants.commons.granule.model.GranuleMap;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.utils.XmlParseUtils;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import com.bonree.ants.extention.topology.TopologyFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.jsoup.nodes.Element;

import java.util.UUID;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年8月8日 下午2:45:00
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 扩展拓扑入口-基线/报警
 * ****************************************************************************
 */
public class ExtentionServer extends EngineServer {

    /**
     * 概述：基线拓扑入口
     *
     * @param args
     */
    public static void main(String[] args) {
        EngineServer storage = new ExtentionServer();
        storage.install(args);
    }

    @Override
    protected void loadAppInfo(XmlParseUtils config) throws Exception {
        Element globalElement = config.getElement("global");// 计算模块的公共参数
        antsConfig.setRedisBatchCount(config.getIntegerValue(globalElement, "redis.batch.count"));
        antsConfig.setSchemaConfigPath(ZKGlobals.SCHEMA_CALC_CONFIG_PATH);
        Element baselineElement = config.getElement("baseline");
        super.initGranuleInfo(config, baselineElement, null); // 初始粒度相关信息
        String[] startupParams; // 启动参数
        if (GlobalContext.BASELINE.equals(antsConfig.getTopologyName())) { // 基线模块的配置参数
            startupParams = config.getStringArray(baselineElement, "baseline.startup.params", Delim.BLANK.value());
            antsConfig.setBaselineExpire(config.getIntegerValue(baselineElement, "baseline.cycle") * 24 * 60 * 60);
            antsConfig.setCalcBaselineInterval(config.getIntegerValue(baselineElement, "calc.baseline.interval"));
            topologyMemory = config.getStringValue(baselineElement, "baseline.memory");
        } else { // 报警模块的配置参数
            Element kafkaElement = config.getElement("kafka");
            KafkaGlobals.ALERT_TOPIC = config.getStringValue(kafkaElement, "alert.result.topic");
            startupParams = config.getStringArray(baselineElement, "alert.startup.params", Delim.BLANK.value());
            antsConfig.setMsgMaxByte(config.getStringValue(baselineElement, "msg.max.bytes"));
            antsConfig.setBaselineCycle(config.getIntegerValue(baselineElement, "baseline.cycle"));
            antsConfig.setCalcAlertInterval(config.getIntegerValue(baselineElement, "calc.alert.interval"));
            topologyMemory = config.getStringValue(baselineElement, "alert.memory");
        }
        TopologyFactory.EXTENTION_PARAMS.setWorkerNum(antsConfig.getSupervisorCount());
        for (String params : startupParams) { // 处理启动参数
            String[] param = params.split(Delim.COLON.value());
            if (param.length != 2) {
                throw new IllegalArgumentException("Startup params format error! " + params);
            }
            String name = param[0].trim();
            String value = param[1].trim();
            if (!TopologyFactory.EXTENTION_PARAMS.containsKey(name)) {
                throw new IllegalArgumentException("Startup params not exists! " + params + ", " + antsConfig.getTopologyName());
            }
            TopologyFactory.EXTENTION_PARAMS.put(name, Integer.valueOf(value));
        }
    }

    @Override
    protected void startup(Config conf) throws Exception {
        GranuleMap graMap = JSON.parseObject(antsConfig.getGranuleMap(), GranuleMap.class);
        conf.setMaxSpoutPending(1); // 设置spout并行度
        conf.put("topology.spout.max.batch.size", graMap.tailValue().getParallel());
        conf.setNumWorkers(TopologyFactory.EXTENTION_PARAMS.getWorkerNum());     // STORM集群进程数
        conf.setNumAckers(TopologyFactory.EXTENTION_PARAMS.getWorkerNum() * 10); // ACK线程数
        if (TopologyFactory.EXTENTION_PARAMS.getAggNum() == null) { // 设置全局汇总线程数
            TopologyFactory.EXTENTION_PARAMS.setAggNum(graMap.tailValue().getBucket() * 2);
        }
        String topologyName = antsConfig.getNamespace() + Delim.UNDERLINE.value() + antsConfig.getTopologyName();
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, String.format(GlobalContext.GC_PARAMS, topologyMemory, topologyName + UUID.randomUUID().toString())); // 拓扑内存
        StormTopology topology = TopologyFactory.initTopology(antsConfig.getTopologyName());
        // 启动topology
        StormSubmitter.submitTopology(topologyName, conf, topology);
    }
}
