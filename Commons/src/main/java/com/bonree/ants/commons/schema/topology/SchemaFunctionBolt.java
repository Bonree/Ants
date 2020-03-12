package com.bonree.ants.commons.schema.topology;

import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.schema.SchemaConfigImpl;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.SchemaManager;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.utils.DateFormatUtils;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

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
public abstract class SchemaFunctionBolt extends BaseFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(SchemaFunctionBolt.class);

    /**
     * 概述：广播配置信息
     *
     * @param schemaInfo 插件配置文件信息
     * @param schema     业务配置文件信息
     * @param createTime 广播配置的时间
     * @throws Exception
     */
    public abstract void broadcast(String schemaInfo, Schema schema, long createTime) throws Exception;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        Commons.initAntsConfig(conf);
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            String schemaConfs = tuple.getStringByField("schemaConfs"); // schema.xml信息
            Schema schemaBiz = (Schema) tuple.getValueByField("schemaBiz"); // schema目录下所有的业务表信息
            Date tmpDate = DateFormatUtils.dayDiff(new Date(), 1);
            long createTime = DateFormatUtils.getDay(tmpDate).getTime();
            broadcast(schemaConfs, schemaBiz, createTime);
        } catch (Exception ex) {
            log.error("SchemaBolt error!", ex);
        }
    }

    /**
     * 概述：schema配置信息处理
     *
     * @param schema     schema配置信息
     * @param createTime 创建时间
     * @throws Exception
     */
    protected void schemaConfig(Schema schema, long createTime) throws Exception {
        schema.setCreateTime(createTime);
        SchemaManager manager = new SchemaConfigImpl(schema);
        manager.saveSchema();
        long limit = SchemaGLobals.SCHEMA_QUEUE.limit();
        long total = SchemaGLobals.SCHEMA_QUEUE.size();
        log.info("broadcast schema, total: {}, fristTime: {}", total, limit);
    }
}
