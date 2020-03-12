package com.bonree.ants.calc.topology.schema;

import com.bonree.ants.calc.plugin.EtlPluginImpl;
import com.bonree.ants.calc.plugin.OperatorPluginImpl;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.utils.XmlParseUtils;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.plugin.PluginManager;
import com.bonree.ants.commons.plugin.model.EtlPlugin;
import com.bonree.ants.commons.plugin.model.OperatorPlugin;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.schema.topology.SchemaFunctionBolt;
import org.jsoup.nodes.Element;
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
 * @Description: 广播schmea配置信息
 * ****************************************************************************
 */
public class BoradcastBolt extends SchemaFunctionBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(BoradcastBolt.class);

    private static AtomicBoolean exists = new AtomicBoolean(false);

    @Override
    public void broadcast(String schemaInfo, Schema schema, long createTime) throws Exception {
        super.schemaConfig(schema, createTime);
        if (GlobalContext.PREPROCESSING.equals(GlobalContext.getConfig().getTopologyName())) {
            etlPluginConfig(schemaInfo, createTime);
            operatorPluginConfig(schemaInfo, createTime);
        }
    }

    /**
     * 概述：etl插件处理
     *
     * @param createTime 创建时间
     * @throws Exception
     */
    private void etlPluginConfig(String schemaInfo, long createTime) throws Exception {
        if (exists.compareAndSet(false, true)) {
            try {
                XmlParseUtils schemaConfig = new XmlParseUtils(schemaInfo);
                Element etlElement = schemaConfig.getElement("etl-plugin");    // storage信息
                String serviceImpl = schemaConfig.getStringValue(etlElement, "etl.service.impl");
                String pluginName = UUID.randomUUID().toString() + ".jar";
                PluginManager plugin = new EtlPluginImpl(pluginName, serviceImpl);
                EtlPlugin etlPlugin = plugin.load(true);
                etlPlugin.setCreateTime(createTime);
                plugin.savePlugin(etlPlugin);
                long fristTime = PluginGlobals.ETL_QUEUE.peek().getCreateTime();
                long total = PluginGlobals.ETL_QUEUE.size();
                log.info("Broadcast etl plugin, total: {}, fristTime: {}, info: {}, name:{}", total, fristTime, serviceImpl, pluginName);
            } finally {
                exists.set(false);
            }
        }
    }

    /**
     * 概述：operator插件处理
     *
     * @param createTime 创建时间
     * @throws Exception
     */
    private void operatorPluginConfig(String schemaInfo, long createTime) throws Exception {
        if (exists.compareAndSet(false, true)) {
            try {
                XmlParseUtils schemaConfig = new XmlParseUtils(schemaInfo);
                Element operatorElement = schemaConfig.getElement("operator-plugin");    // storage信息
                boolean useOperator = Boolean.valueOf(schemaConfig.getAttr(operatorElement, "use"));
                GlobalContext.getConfig().setUseOperatorPlugin(useOperator);
                if (useOperator) {
                    String serviceImpl = schemaConfig.getStringValue(operatorElement, "operator.service.impl");
                    String pluginName = UUID.randomUUID().toString() + ".jar";
                    PluginManager plugin = new OperatorPluginImpl(pluginName, serviceImpl);
                    OperatorPlugin etlPlugin = plugin.load(true);
                    etlPlugin.setCreateTime(createTime);
                    plugin.savePlugin(etlPlugin);
                    long fristTime = PluginGlobals.OPERATOR_QUEUE.peek().getCreateTime();
                    long total = PluginGlobals.OPERATOR_QUEUE.size();
                    log.info("Broadcast operator plugin, total: {}, fristTime: {}, info: {}, name:{}", total, fristTime, createTime, pluginName);
                }
            } finally {
                exists.set(false);
            }
        }
    }
}
