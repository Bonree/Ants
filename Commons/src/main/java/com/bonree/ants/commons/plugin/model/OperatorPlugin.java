package com.bonree.ants.commons.plugin.model;

import java.io.Serializable;

import com.bonree.ants.plugin.operator.IOperatorService;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年6月27日 下午3:35:18
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 自定义算子插件实体类
 *****************************************************************************
 */
public class OperatorPlugin implements Serializable {

    private static final long serialVersionUID = 1L;

    private long createTime;        // operator插件创建时间

    private IOperatorService operatorService; // operator插件

    private String pluginName;      // 插件名称

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public IOperatorService getOperatorService() {
        return operatorService;
    }

    public void setOperatorService(IOperatorService operatorService) {
        this.operatorService = operatorService;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }
}
