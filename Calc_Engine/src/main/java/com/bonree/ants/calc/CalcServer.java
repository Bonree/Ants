package com.bonree.ants.calc;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.calc.topology.TopologyFactory;
import com.bonree.ants.calc.utils.FinalUtils;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.engine.EngineServer;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.granule.model.GranuleMap;
import com.bonree.ants.commons.kafka.KafkaCommons;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.schema.SchemaConfigImpl;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.SchemaManager;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.utils.XmlParseUtils;
import com.bonree.ants.commons.zookeeper.ZKCommons;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月10日 上午11:46:18
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 计算引擎主启动类
 * ****************************************************************************
 */
public class CalcServer extends EngineServer {

    private static final Logger log = LoggerFactory.getLogger(CalcServer.class);

    /**
     * 概述：业务数量
     */
    private int bizCount;

    /**
     * 概述：计算程序入口
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        EngineServer calcServer = new CalcServer();
        calcServer.install(args);
    }

    /**
     * 概述：上传schema配置文件到zk
     */
    @Override
    protected void uploadSchemaInfo(XmlParseUtils config) throws Exception {
        loadPluginInfo(config, true);
        XmlParseUtils schemaConfig = new XmlParseUtils(new File(antsHome + SchemaGLobals.SCHEMA_FILE_PATH));
        Element schemaElement = schemaConfig.getElement("ants");
        schemaConfig.addElement(schemaElement, config.getElement("etl-plugin").toString());
        schemaConfig.addElement(schemaElement, config.getElement("operator-plugin").toString());
        schemaConfig.addElement(schemaElement, config.getElement("default-storage-plugin").toString());
        schemaConfig.addElements(schemaElement, config.getElements("custom-storage-plugin"));
        byte[] bytes = schemaConfig.toString().getBytes(StandardCharsets.UTF_8); // 加载schema.xml配置信息
        ZKCommons.instance.setData(ZKGlobals.SCHEMA_CONFIG_PATH, bytes);

        SchemaManager manager = new SchemaConfigImpl();
        manager.loadBiz(antsHome); // 加载业务表配置信息
        manager.loadDesc(schemaConfig);
        String schemaJson = JSON.toJSONString(manager.getSchema());
        ZKCommons.instance.setData(ZKGlobals.SCHEMA_CALC_CONFIG_PATH, schemaJson);
    }

    /**
     * 概述：加载应用配置信息
     */
    @Override
    protected void loadAppInfo(XmlParseUtils config) throws Exception {
        // 计算模块的配置参数
        Element calcElement = config.getElement("calc");
        String topologyName = antsConfig.getTopologyName();
        int splitNumber = config.getIntegerValue(calcElement, "split.granule.number");
        Integer[] granuleArr = config.getIntArray(calcElement, "granule", Delim.COMMA.value());
        this.initTopologyInfo(topologyName, splitNumber, granuleArr);
        // 计算模块的公共参数
        Element globalElement = config.getElement("global");
        antsConfig.setRedisBatchCount(config.getIntegerValue(globalElement, "redis.batch.count"));
        antsConfig.setKafkaBatchCount(config.getIntegerValue(globalElement, "kafka.batch.count"));
        // kafka信息
        Element kafkaElement = config.getElement("kafka");
        antsConfig.setGroupId(config.getStringValue(kafkaElement, "group.id"));
        boolean sourceTopicUseNamespace = config.getBooleanValue(kafkaElement, "source.topic.use.namespace");
        this.loadPluginInfo(config, false);
        Integer[] currGranuleArr;
        if (splitNumber == 0 || GlobalContext.BIG_GRANULE.equals(topologyName)) {
            currGranuleArr = Arrays.copyOfRange(granuleArr, splitNumber, granuleArr.length);
        } else {
            currGranuleArr = Arrays.copyOf(granuleArr, splitNumber);
        }
        Set<Integer> currGranuleSets = new HashSet<>(Arrays.asList(currGranuleArr));
        super.initGranuleInfo(config, calcElement, currGranuleSets); // 初始粒度相关信息
        String[] startupParams;
        if (GlobalContext.PREPROCESSING.equals(topologyName)) {
            antsConfig.setCalcPreprocessInterval(config.getIntegerValue(calcElement, "calc.preprocessing.interval"));
            topologyMemory = config.getStringValue(calcElement, "preprocessing.memory");
            antsConfig.setMaxPollCount(config.getStringValue(calcElement, "max.poll.count"));
            startupParams = config.getStringArray(calcElement, "preprocessing.startup.params", Delim.BLANK.value());
            TopologyFactory.PREPROCESS_PARAMS.setWorkerNum(antsConfig.getSupervisorCount());
        } else {
            antsConfig.setCalcGranuleInterval(config.getIntegerValue(calcElement, "calc.granule.interval"));
            antsConfig.setCalcBigGranuleInterval(config.getIntegerValue(calcElement, "calc.big.granule.interval"));
            antsConfig.setDelayCalcTime(config.getIntegerValue(calcElement, "delay.calc.time"));
            startupParams = config.getStringArray(calcElement, "granule.startup.params", Delim.BLANK.value());
            TopologyFactory.GRANULE_PARAMS.setWorkerNum(antsConfig.getSupervisorCount());
            topologyMemory = config.getStringValue(calcElement, GlobalContext.GRANULE.equals(topologyName) ? "granule.memory" : "big.granule.memory");
        }
        antsConfig.setMsgMaxByte(config.getStringValue(calcElement, "msg.max.bytes"));
        String[] resultTopicArray = config.getAttrArray(config.getElement("default-storage-plugin"), "topic", Delim.COMMA.value());
        if (resultTopicArray.length >= 1) {
            antsConfig.setResultTopic(resultTopicArray[0]);
        }
        if (resultTopicArray.length >= 2) {
            antsConfig.setResultDayTopic(resultTopicArray[1]);
        }
        for (String params : startupParams) {// 处理启动参数
            String[] param = params.split(Delim.COLON.value());
            if (param.length != 2) {
                throw new IllegalArgumentException("Startup params format error! " + params);
            }
            String name = param[0].trim();
            String value = param[1].trim();
            if (GlobalContext.PREPROCESSING.equals(topologyName)) {
                if (!TopologyFactory.PREPROCESS_PARAMS.containsKey(name)) {
                    throw new IllegalArgumentException("Startup params not exists! " + params + ", " + topologyName);
                }
                TopologyFactory.PREPROCESS_PARAMS.put(name, Integer.valueOf(value));
            } else {
                if (!TopologyFactory.GRANULE_PARAMS.containsKey(name)) {
                    throw new IllegalArgumentException("Startup params not exists! " + params + ", " + topologyName);
                }
                TopologyFactory.GRANULE_PARAMS.put(name, Integer.valueOf(value));
            }
        }
        String[] topicArray = config.getStringArray(kafkaElement, "source.topic", Delim.COMMA.value());
        KafkaGlobals.SOURCE_TOPIC = sourceTopicUseNamespace ? KafkaCommons.topicNamespace(Arrays.asList(topicArray)) : Arrays.asList(topicArray);
    }

    /**
     * 概述：初始当前拓扑的相关信息
     *
     * @param topologyName 当前拓扑名称
     * @param splitNumber  当前拓扑分割粒度的索引
     * @param granuleArr   当前拓扑粒度集合
     */
    private void initTopologyInfo(String topologyName, int splitNumber, Integer[] granuleArr) throws Exception {
        antsConfig.setNextTopology(GlobalContext.BIG_GRANULE);
        if (GlobalContext.GRANULE.equals(topologyName)) {
            antsConfig.setLastTopology(GlobalContext.PREPROCESSING);
        }
        if (GlobalContext.BIG_GRANULE.equals(topologyName)) {
            antsConfig.setLastTopology(GlobalContext.GRANULE);
        }
        if (GlobalContext.PREPROCESSING.equals(topologyName)) {
            antsConfig.setNextTopology(GlobalContext.GRANULE);
        }
        if (splitNumber == 0) { // 只有大粒度汇总
            if (GlobalContext.PREPROCESSING.equals(topologyName)) {
                antsConfig.setNextTopology(GlobalContext.BIG_GRANULE);
            }
            if (GlobalContext.BIG_GRANULE.equals(topologyName)) {
                antsConfig.setLastTopology(GlobalContext.PREPROCESSING);
            }
            if (GlobalContext.GRANULE.equals(topologyName)) { // granule不需要启动
                throw new Exception("split.granule.number is " + splitNumber + ", " + topologyName + " no need startup !!");
            }
        }
        if (splitNumber == granuleArr.length) { // 只有小粒度汇总
            if (GlobalContext.GRANULE.equals(topologyName)) {
                antsConfig.setNextTopology(GlobalContext.GRANULE);
            }
            if (GlobalContext.BIG_GRANULE.equals(topologyName)) { // bigGranule不需要启动
                throw new Exception("split.granule.number is " + splitNumber + ", " + topologyName + " no need startup !!");
            }
        }
    }

    /**
     * 概述：加载插件信息
     *
     * @param config      配置文件对象
     * @param loadStorage 是否加载存储插件
     */
    private void loadPluginInfo(XmlParseUtils config, boolean loadStorage) throws Exception {
        antsConfig.setSchemaConfigPath(ZKGlobals.SCHEMA_CALC_CONFIG_PATH);
        // etl信息
        Element etlElement = config.getElement("etl-plugin");
        antsConfig.setEtlPluginName(UUID.randomUUID().toString() + ".jar");
        antsConfig.setEtlServiceImpl(config.getStringValue(etlElement, "etl.service.impl"));
        savePluginToRedis(config, etlElement, RedisGlobals.ETL_PLUGIN_KEY, "etl.plugin.path");
        // operator插件信息
        Element operatorElement = config.getElement("operator-plugin");
        boolean useOperator = Boolean.valueOf(config.getAttr(operatorElement, "use"));
        antsConfig.setUseOperatorPlugin(useOperator);
        if (useOperator) {
            antsConfig.setOperatorPluginName(UUID.randomUUID().toString() + ".jar");
            antsConfig.setOperatorServiceImpl(config.getStringValue(operatorElement, "operator.service.impl"));
            savePluginToRedis(config, operatorElement, RedisGlobals.OPERATOR_PLUGIN_KEY, "operator.plugin.path");
        }
        // storage插件信息
        if (loadStorage) {
            Element defaultEle = config.getElement("default-storage-plugin");
            boolean useStorage = Boolean.valueOf(config.getAttr(defaultEle, "use"));
            if (useStorage) { // 保存指标存储插件
                savePluginToRedis(config, defaultEle, RedisGlobals.STORAGE_PLUGIN_KEY, "storage.plugin.path");
            }
            Elements customEles = config.getElements("custom-storage-plugin");
            if (customEles != null && !customEles.isEmpty()) {
                for (Element element : customEles) { // 保存自定义数据存储插件
                    String storageKey = RedisGlobals.STORAGE_PLUGIN_KEY + "_" + config.getAttr(element, "topic");
                    savePluginToRedis(config, element, storageKey, "storage.plugin.path");
                }
            }
        }
    }

    /**
     * 概述：加载业务描述信息
     */
    @Override
    protected void loadSchemaInfo() throws Exception {
        XmlParseUtils config = new XmlParseUtils(new File(antsHome + SchemaGLobals.SCHEMA_FILE_PATH));
        SchemaManager manager = new SchemaConfigImpl();
        manager.loadBiz(antsHome);// 加载业务表配置信息
        manager.loadDesc(config);
        Schema schema = manager.getSchema();
        bizCount = schema.getData().size();
        String schemaJson = JSON.toJSONString(schema);
        ZKCommons.instance.setData(ZKGlobals.SCHEMA_CONFIG_PATH, "");
        ZKCommons.instance.setData(ZKGlobals.SCHEMA_CALC_CONFIG_PATH, schemaJson);
        log.info("Set calc schema to zk. {}", schemaJson);
    }

    /**
     * 概述：启动storm计算
     */
    @Override
    protected void startup(Config conf) throws Exception {
        int workernum;
        String tmpTopologyName = antsConfig.getTopologyName();
        if (GlobalContext.PREPROCESSING.equals(tmpTopologyName)) {
            workernum = TopologyFactory.PREPROCESS_PARAMS.getWorkerNum();
            if (TopologyFactory.PREPROCESS_PARAMS.getSpoutPendingNum() == null) {
                TopologyFactory.PREPROCESS_PARAMS.setSpoutPendingNum(1);
            }
            conf.setMaxSpoutPending(TopologyFactory.PREPROCESS_PARAMS.getSpoutPendingNum()); // 设置spout并行度
            conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, antsConfig.getCalcPreprocessInterval()); // 设置etl拓扑的频率
            TopologyFactory.PREPROCESS_PARAMS.setSpoutThreadNum(workernum);
            if (TopologyFactory.PREPROCESS_PARAMS.getShuffleNum() == null) { // 设置预处理汇总线程数
                TopologyFactory.PREPROCESS_PARAMS.setShuffleNum(workernum * 20);
            }
            if (TopologyFactory.PREPROCESS_PARAMS.getGroupNum() == null) { // 设置全局汇总线程数
                TopologyFactory.PREPROCESS_PARAMS.setGroupNum(bizCount * 2);
            }
            if (TopologyFactory.PREPROCESS_PARAMS.getBatchNum() == null) { // 预处理拓扑批量处理数据的数量
                TopologyFactory.PREPROCESS_PARAMS.setBatchNum(500);
            }
        } else {
            workernum = TopologyFactory.GRANULE_PARAMS.getWorkerNum();
            GranuleMap graMap = JSON.parseObject(antsConfig.getGranuleMap(), GranuleMap.class);
            conf.setMaxSpoutPending(1); // 设置spout并行度
            conf.put("topology.spout.max.batch.size", graMap.tailValue().getParallel());
            if (TopologyFactory.GRANULE_PARAMS.getAggNum() == null) { // 设置全局汇总线程数
                TopologyFactory.GRANULE_PARAMS.setAggNum(graMap.tailValue().getBucket() * 2);
            }
        }
        conf.setNumWorkers(workernum);     // STORM集群进程数
        conf.setNumAckers(workernum * bizCount / 2); // ACK线程数
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, FinalUtils.MSG_TIMEOUT);
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, FinalUtils.SESSION_TIMEOUT);
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, FinalUtils.CLIENT_TIMEOUT);
        String topologyName = antsConfig.getNamespace() + Delim.UNDERLINE.value() + tmpTopologyName;
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, String.format(GlobalContext.GC_PARAMS, topologyMemory, topologyName + UUID.randomUUID().toString())); // 拓扑内存
        StormTopology topology = TopologyFactory.initTopology(tmpTopologyName);
        if (topology == null) {
            throw new Exception("Topology name input error !!!");
        }
        // 启动topology
        StormSubmitter.submitTopology(topologyName, conf, topology);
    }
}
