package com.bonree.ants.storage.plugin;

import com.bonree.ants.commons.plugin.ClassLoader;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.plugin.PluginManager;
import com.bonree.ants.commons.plugin.model.StoragePlugin;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.plugin.storage.IStorageService;

import java.util.Date;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年5月28日 下午6:25:39
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 存储插件管理
 *****************************************************************************
 */
public class StoragePluginImpl extends PluginManager {

    /**
     * 插件jar名称
     */
    private String pluginName;

    /**
     * 初始化
     * @param pluginName 插件jar名称
     * @param serviceImpl 插件实现类完成路径名称
     * @param pluginKey 插件存在redis中的key名称
     */
    public StoragePluginImpl(String pluginName, String serviceImpl, String pluginKey) {
        super(PluginGlobals.STORAGE_PATH, pluginName, pluginKey, serviceImpl);
        this.pluginName = pluginName;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> T getPlugin(String serviceImpl) throws Exception {
        IStorageService stroageService = ClassLoader.instance.getPlugin(serviceImpl, IStorageService.class);
        StoragePlugin storagePlugin = new StoragePlugin();
        storagePlugin.setPluginName(pluginName);
        storagePlugin.setStorageService(stroageService);
        long currTime = DateFormatUtils.getDay(new Date()).getTime();
        storagePlugin.setCreateTime(currTime);
        return (T) storagePlugin;
    }

    @Override
    public <T> void savePlugin(T plugin) throws Exception {
        save(PluginGlobals.STORAGE_QUEUE, (StoragePlugin) plugin);
    }

}
