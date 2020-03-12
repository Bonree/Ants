package com.bonree.ants.commons.plugin.model;

import java.io.Serializable;

import com.bonree.ants.plugin.etl.IEtlService;
import com.bonree.ants.plugin.etl.model.SchemaConfig;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年5月10日 下午6:05:48
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: etl插件实体类
 *****************************************************************************
 */
public class EtlPlugin implements Serializable {

    private static final long serialVersionUID = 1L;

    private long createTime;        // etl插件创建时间

    private IEtlService etlService; // etl插件

    private String pluginName;      // 插件名称

    private SchemaConfig config = new SchemaConfig(); // schema配置信息

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public IEtlService getEtlService() {
        return etlService;
    }

    public void setEtlService(IEtlService etlService) {
        this.etlService = etlService;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public SchemaConfig getConfig() {
        return config;
    }

    public void setConfig(SchemaConfig config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "{time: " + createTime + ", name: " + pluginName + ", config: " + config + "}";
    }
}
