package com.bonree.ants.storage;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.engine.EngineServer;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.kafka.KafkaCommons;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.schema.SchemaConfigImpl;
import com.bonree.ants.commons.schema.SchemaManager;
import com.bonree.ants.commons.utils.XmlParseUtils;
import com.bonree.ants.commons.zookeeper.ZKCommons;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import com.bonree.ants.storage.topology.TopologyFactory;
import com.bonree.ants.storage.utils.DBUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月16日 上午9:51:28
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 入库引擎主启动类
 * ****************************************************************************
 */
public class StorageServer extends EngineServer {

    private static final Logger log = LoggerFactory.getLogger(StorageServer.class);

    private List<String> topics = new ArrayList<>();

    /**
     * 概述：存储程序入口
     *
     * @param args 入口参数
     */
    public static void main(String[] args) {
        EngineServer storage = new StorageServer();
        storage.install(args);
    }

    /**
     * 概述：加载应用配置信息
     */
    @Override
    protected void loadAppInfo(XmlParseUtils config) throws Exception {
        boolean useStorage = false;
        if (StringUtils.isEmpty(storageTopic)) { // 指标数据存储
            Element storageElement = config.getElement("default-storage-plugin");    // storage指标信息
            String[] topicArray = config.getAttrArray(storageElement, "topic", Delim.COMMA.value());
            topics.addAll(Arrays.asList(topicArray));
            useStorage = Boolean.valueOf(config.getAttr(storageElement, "use"));
            if (useStorage) {
                antsConfig.setStorageServiceImpl(config.getStringValue(storageElement, "storage.service.impl"));
                antsConfig.setStoragePluginName(UUID.randomUUID().toString() + ".jar");
                savePluginToRedis(config, storageElement, RedisGlobals.STORAGE_PLUGIN_KEY, "storage.plugin.path");
            } else {
                DBUtils.initDbParams(config);// 加载db配置信息
            }
        } else { // 自定义数据存储
            Elements customEles = config.getElements("custom-storage-plugin");// storage自定义数据信息
            if (customEles == null || customEles.isEmpty()) {
                throw new Exception("No load custom-storage-plugin!!! topic: " + storageTopic);
            }
            for (Element element : customEles) {
                String topic = config.getAttr(element, "topic");
                if (topic.equals(storageTopic)) { // 保存自定义数据插件
                    useStorage = true;
                    topics.add(storageTopic);
                    antsConfig.setCustomStorageTopic(storageTopic);
                    antsConfig.setStorageServiceImpl(config.getStringValue(element, "storage.service.impl"));
                    antsConfig.setStoragePluginName(UUID.randomUUID().toString() + ".jar");
                    String storageKey = RedisGlobals.STORAGE_PLUGIN_KEY + "_" + storageTopic;
                    savePluginToRedis(config, element, storageKey, "storage.plugin.path");
                    break;
                }
            }
        }
        if (topics == null || topics.isEmpty()) {
            throw new Exception("Load custom-storage-plugin topic is empty !!! topic： " + storageTopic);
        }
        antsConfig.setUseStoragePlugin(useStorage);
        // 入库模块的配置参数
        TopologyFactory.STORAGE_PARAMS.setWorkerNum(antsConfig.getSupervisorCount());
        Element insertElement = config.getElement("storage");
        String[] startupParams = config.getStringArray(insertElement, "storage.startup.params", Delim.BLANK.value());
        for (String params : startupParams) {
            String[] param = params.split(Delim.COLON.value());
            if (param.length != 2) {
                throw new IllegalArgumentException("Startup params format error! " + params);
            }
            String name = param[0].trim();
            String value = param[1].trim();
            if (!TopologyFactory.STORAGE_PARAMS.containsKey(name)) {
                throw new IllegalArgumentException("Startup params not exists! " + params + ", " + antsConfig.getTopologyName());
            }
            TopologyFactory.STORAGE_PARAMS.put(name, Integer.valueOf(value));
        }
        antsConfig.setMsgMaxByte(config.getStringValue(insertElement, "msg.max.bytes"));
        antsConfig.setMaxPollCount(config.getStringValue(insertElement, "max.poll.count"));
        antsConfig.setSchemaConfigPath(ZKGlobals.SCHEMA_INSERT_CONFIG_PATH);
        topologyMemory = config.getStringValue(insertElement, "storage.memory");
    }

    /**
     * 概述：加载业务描述信息
     */
    @Override
    protected void loadSchemaInfo() throws Exception {
        if (!antsConfig.useStoragePlugin()) {
            SchemaManager manager = new SchemaConfigImpl();
            manager.loadBiz(antsHome);
            manager.initDbSql(); // 获取每个业务入库的sql
            String schemaJson = JSON.toJSONString(manager.getSchema());
            ZKCommons.instance.setData(ZKGlobals.SCHEMA_INSERT_CONFIG_PATH, schemaJson);
            log.info("Set storage schema to zk. {}", schemaJson);
        }
    }

    /**
     * 概述：启动storm计算
     */
    @Override
    protected void startup(Config conf) throws Exception {
        int workerNum = TopologyFactory.STORAGE_PARAMS.getWorkerNum();
        if (TopologyFactory.STORAGE_PARAMS.getSpoutThreadNum() == null) {
            TopologyFactory.STORAGE_PARAMS.setSpoutThreadNum(workerNum);
        }
        if (TopologyFactory.STORAGE_PARAMS.getShuffleNum() == null) {
            TopologyFactory.STORAGE_PARAMS.setShuffleNum(workerNum * 20);
        }
        if (TopologyFactory.STORAGE_PARAMS.getSpoutPendingNum() == null) {
            TopologyFactory.STORAGE_PARAMS.setSpoutPendingNum(workerNum);
        }
        topics.add(KafkaGlobals.CONNECTION_FAIL_TOPIC);
        StormTopology topology = TopologyFactory.initTopology(KafkaCommons.topicNamespace(topics), storageTopic);

        conf.setNumWorkers(workerNum);     // STORM集群进程数
        conf.setNumAckers(workerNum * 10); // ACK线程数
        conf.setMaxSpoutPending(TopologyFactory.STORAGE_PARAMS.getSpoutPendingNum()); // 设置spout并行度
        String topologyName = antsConfig.getNamespace() + Delim.UNDERLINE.value() + antsConfig.getTopologyName();
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, String.format(GlobalContext.GC_PARAMS, topologyMemory, topologyName + UUID.randomUUID().toString())); // 拓扑内存
        StormSubmitter.submitTopology(topologyName, conf, topology);// 启动topology
    }
}
