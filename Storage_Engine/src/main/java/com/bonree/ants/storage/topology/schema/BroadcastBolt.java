package com.bonree.ants.storage.topology.schema;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.utils.XmlParseUtils;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.plugin.PluginManager;
import com.bonree.ants.commons.plugin.model.StoragePlugin;
import com.bonree.ants.commons.schema.SchemaConfigImpl;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.SchemaManager;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.schema.topology.SchemaRichBolt;
import com.bonree.ants.storage.plugin.StoragePluginImpl;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年5月9日 下午7:08:31
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 广播storage的schmea配置信息
 * ****************************************************************************
 */
public class BroadcastBolt extends SchemaRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(BroadcastBolt.class);
    private static AtomicBoolean exists = new AtomicBoolean(false);

    private final String storageTopic;

    public BroadcastBolt(String topic) {
        this.storageTopic = topic;
    }

    @Override
    public void broadcast(String schemaInfo, Schema schema, long createTime) {
        try {
            storagePluginConfig(schemaInfo, schema, createTime);
        } catch (Exception ex) {
            log.error("Storage broadcast error!", ex);
        }
    }

    /**
     * 概述：storage插件处理
     *
     * @param schemaInfo schema.xml配置信息
     * @param schema     schema目录下的业务表信息
     * @throws Exception
     */
    private void storagePluginConfig(String schemaInfo, Schema schema, long createTime) throws Exception {
        if (exists.compareAndSet(false, true)) {
            try {
                XmlParseUtils config = new XmlParseUtils(schemaInfo);
                StoragePlugin storagePlugin = null;
                PluginManager plugin = null;
                String pluginName = "";
                // storage配置参数
                if (StringUtils.isEmpty(storageTopic)) {
                    Element storageElement = config.getElement("default-storage-plugin");
                    boolean useStroage = Boolean.valueOf(config.getAttr(storageElement, "use"));
                    GlobalContext.getConfig().setUseStoragePlugin(useStroage);
                    pluginName = UUID.randomUUID().toString() + ".jar";
                    String serviceImpl = config.getValue(storageElement, "storage.service.impl");
                    plugin = new StoragePluginImpl(pluginName, serviceImpl, RedisGlobals.STORAGE_PLUGIN_KEY);
                    if (useStroage) {// 指标数据storage插件
                        storagePlugin = plugin.load(true);
                    } else {
                        storagePlugin = new StoragePlugin();
                        storageSchemaConfig(schema, createTime);
                    }
                } else {
                    Elements customEles = config.getElements("custom-storage-plugin");// storage自定义数据信息
                    if (customEles == null || customEles.isEmpty()) {
                        throw new Exception("No load custom-storage-plugin!!! topic: " + storageTopic);
                    }
                    for (Element element : customEles) {
                        String topic = config.getAttr(element, "topic");
                        if (topic.equals(storageTopic)) { // 保存自定义数据插件
                            GlobalContext.getConfig().setUseStoragePlugin(true);
                            String serviceImpl = config.getValue(element, "storage.service.impl");
                            pluginName = UUID.randomUUID().toString() + ".jar";
                            String pulginKey = RedisGlobals.STORAGE_PLUGIN_KEY + "_" + storageTopic;
                            plugin = new StoragePluginImpl(pluginName, serviceImpl, pulginKey);
                            storagePlugin = plugin.load(true);
                            break;
                        }
                    }
                }
                if (plugin == null || storagePlugin == null) {
                    throw new Exception("Load storage-plugin is empty !!! topic： " + storageTopic);
                }
                storagePlugin.setCreateTime(createTime);
                plugin.savePlugin(storagePlugin);
                long fristTime = PluginGlobals.STORAGE_QUEUE.peek().getCreateTime();
                long total = PluginGlobals.STORAGE_QUEUE.size();
                log.info("Broadcast storage plugin, total: {}, fristTime: {}, info: {}, name:{}", total, fristTime, createTime, pluginName);
            } finally {
                exists.set(false);
            }
        }
    }

    /**
     * 概述：schema业务表信息处理
     *
     * @param schema     schema目录下的业务表信息
     * @param createTime 创建时间
     * @throws Exception
     */
    private void storageSchemaConfig(Schema schema, long createTime) throws Exception {
        schema.setCreateTime(createTime);
        SchemaManager manager = new SchemaConfigImpl(schema); // schema配置信息
        manager.initDbSql();
        manager.saveSchema();
        long fristTime = SchemaGLobals.SCHEMA_QUEUE.peek().getCreateTime();
        long total = SchemaGLobals.SCHEMA_QUEUE.size();
        log.info("Broadcast schema, total: {}, fristTime: {}, info: {}", total, fristTime, JSON.toJSONString(manager.getSchema().getData().keySet()));
    }
}
