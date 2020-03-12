package com.bonree.ants.commons.schema.topology;

import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.utils.DateFormatUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
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
public abstract class SchemaRichBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(SchemaRichBolt.class);

    /**
     * 概述：广播配置信息
     *
     * @param schemaInfo 插件配置文件信息
     * @param schema     业务配置文件信息
     * @param createTime 广播配置的时间
     */
    public abstract void broadcast(String schemaInfo, Schema schema, long createTime) throws Exception;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Commons.initAntsConfig(stormConf);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String schemaConfs = input.getStringByField("schemaConfs"); // schema.xml信息
            Schema schemaBiz = (Schema) input.getValueByField("schemaBiz"); // schema目录下所有的业务表信息
            Date tmpDate = DateFormatUtils.dayDiff(new Date(), 1);
            long createTime = DateFormatUtils.getDay(tmpDate).getTime();
            broadcast(schemaConfs, schemaBiz, createTime);
        } catch (Exception ex) {
            log.error("SchemaBolt error!", ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
