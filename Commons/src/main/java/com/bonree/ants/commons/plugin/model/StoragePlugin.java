package com.bonree.ants.commons.plugin.model;

import java.io.Serializable;

import com.bonree.ants.plugin.storage.IStorageService;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年6月27日 下午3:35:48
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 存储插件实体类
 *****************************************************************************
 */
public class StoragePlugin implements Serializable {

    private static final long serialVersionUID = 1L;

    private long createTime;        // storage插件创建时间

    private IStorageService storageService; // storage插件

    private String pluginName;      // 插件名称

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public IStorageService getStorageService() {
        return storageService;
    }

    public void setStorageService(IStorageService storageService) {
        this.storageService = storageService;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }
}
