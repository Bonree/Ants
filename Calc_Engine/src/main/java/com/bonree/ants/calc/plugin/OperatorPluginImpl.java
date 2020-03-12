package com.bonree.ants.calc.plugin;

import java.util.Date;

import com.bonree.ants.commons.plugin.ClassLoader;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.plugin.PluginManager;
import com.bonree.ants.commons.plugin.model.OperatorPlugin;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.plugin.operator.IOperatorService;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年9月4日 上午10:07:35
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 自定义算子管理
 *****************************************************************************
 */
public class OperatorPluginImpl extends PluginManager {
    /**
     * 插件jar名称
     */
    private String pluginName;
    
    /**
     * 初始化
     * @param pluginName 插件jar名称
     * @param serviceImpl 插件实现类完成路径名称
     */
    public OperatorPluginImpl(String pluginName, String serviceImpl) {
        super(PluginGlobals.OPERATOR_PATH, pluginName, RedisGlobals.OPERATOR_PLUGIN_KEY, serviceImpl);
        this.pluginName = pluginName;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T getPlugin(String serviceImpl) throws Exception {
        IOperatorService operatorService = ClassLoader.instance.getPlugin(serviceImpl, IOperatorService.class);
        OperatorPlugin operatorPlugin = new OperatorPlugin();
        operatorPlugin.setPluginName(pluginName);
        operatorPlugin.setOperatorService(operatorService);
        long currTime = DateFormatUtils.getDay(new Date()).getTime();
        operatorPlugin.setCreateTime(currTime);
        return (T) operatorPlugin;
    }

    @Override
    public <T> void savePlugin(T plugin) throws Exception {
        save(PluginGlobals.OPERATOR_QUEUE, (OperatorPlugin) plugin);
    }
}
