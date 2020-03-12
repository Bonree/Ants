package com.bonree.ants.calc.plugin;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.plugin.ClassLoader;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.plugin.PluginManager;
import com.bonree.ants.commons.plugin.model.EtlPlugin;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.schema.model.SchemaFields;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.zookeeper.ZKCommons;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import com.bonree.ants.plugin.etl.IEtlService;
import com.bonree.ants.plugin.etl.model.SchemaConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年5月26日 下午5:24:18
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: ETL插件管理
 * ****************************************************************************
 */
public class EtlPluginImpl extends PluginManager {
    private static final Logger log = LoggerFactory.getLogger(EtlPluginImpl.class);

    /**
     * 插件jar名称
     */
    private String pluginName;

    /**
     * 初始化
     *
     * @param pluginName  插件jar名称
     * @param serviceImpl 插件实现类完成路径名称
     */
    public EtlPluginImpl(String pluginName, String serviceImpl) {
        super(PluginGlobals.ETL_PATH, pluginName, RedisGlobals.ETL_PLUGIN_KEY, serviceImpl);
        this.pluginName = pluginName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getPlugin(String serviceImpl) throws Exception {
        IEtlService etlService = ClassLoader.instance.getPlugin(serviceImpl, IEtlService.class);
        EtlPlugin etlPlugin = new EtlPlugin();
        etlPlugin.setPluginName(pluginName);
        etlPlugin.setEtlService(etlService);
        long currTime = DateFormatUtils.getDay(new Date()).getTime();
        etlPlugin.setCreateTime(currTime);
        etlPlugin.setConfig(loadSchema());
        return (T) etlPlugin;
    }

    @Override
    public <T> void savePlugin(T plugin) throws Exception {
        EtlPlugin etlPlugin = (EtlPlugin) plugin;
        etlPlugin.getEtlService().init(JedisClient.init()); // 初始化参数
        EtlPlugin beforePlugin = save(PluginGlobals.ETL_QUEUE, etlPlugin); // 保存新插件
        if (beforePlugin != null) {
            log.info("release plugin, time: {}, name: {}", beforePlugin.getCreateTime(), beforePlugin.getPluginName());
            beforePlugin.getEtlService().close(); // 释放旧etl资源
            beforePlugin = null;
            System.gc();
        }
    }

    /**
     * 加载schema业务配置信息
     *
     * @throws Exception
     */
    private SchemaConfig loadSchema() throws Exception {
        SchemaConfig config = new SchemaConfig();
        byte[] schemaBytes = ZKCommons.instance.getData(ZKGlobals.SCHEMA_CALC_CONFIG_PATH);
        Schema schema = JSON.parseObject(new String(schemaBytes, StandardCharsets.UTF_8), Schema.class);
        Map<String, SchemaData> map = schema.getData();
        config.setType(schema.getType());
        config.setRelation(schema.getRelation());
        String tableName;
        SchemaConfig.Field field;
        for (Entry<String, SchemaData> entry : map.entrySet()) {
            tableName = entry.getKey();
            for (SchemaFields fields : entry.getValue().getFieldsList()) {
                field = new SchemaConfig.Field();
                if (StringUtils.isNotEmpty(fields.getSource())) {
                    field.setSource(fields.getSource());
                }
                field.setName(fields.getName());
                field.setType(fields.getValueType());
                config.addConfig(tableName, field);
            }
        }
        return config;
    }
}
