package com.bonree.ants.extention.topology.schema;

import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.schema.topology.SchemaFunctionBolt;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/9/20 14:44
 * @Author <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Dscription 广播schmea配置信息
 * *****************************************************************************
 */
public class BroadcastBolt extends SchemaFunctionBolt {

    private static final long serialVersionUID = 1L;

    @Override
    public void broadcast(String schemaInfo, Schema schema, long createTime) throws Exception {
        schemaConfig(schema, createTime);
    }
}
